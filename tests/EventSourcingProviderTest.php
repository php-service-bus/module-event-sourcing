<?php

/**
 * Event Sourcing implementation module.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\EventSourcingModule\Tests;

use function Amp\Promise\wait;
use function ServiceBus\Storage\Sql\fetchOne;
use Amp\Loop;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use ServiceBus\EventSourcing\Aggregate;
use ServiceBus\EventSourcing\EventStream\EventStreamRepository;
use ServiceBus\EventSourcing\EventStream\Serializer\DefaultEventSerializer;
use ServiceBus\EventSourcing\EventStream\Store\EventStreamStore;
use ServiceBus\EventSourcing\EventStream\Store\SqlEventStreamStore;
use ServiceBus\EventSourcing\Snapshots\Snapshotter;
use ServiceBus\EventSourcing\Snapshots\Store\SnapshotStore;
use ServiceBus\EventSourcing\Snapshots\Store\SqlSnapshotStore;
use ServiceBus\EventSourcing\Snapshots\Triggers\SnapshotVersionTrigger;
use ServiceBus\EventSourcing\Tests\stubs\TestAggregate;
use ServiceBus\EventSourcing\Tests\stubs\TestAggregateId;
use ServiceBus\EventSourcingModule\EventSourcingProvider;
use ServiceBus\EventSourcingModule\Exceptions\DuplicateAggregate;
use ServiceBus\EventSourcingModule\Exceptions\RevertAggregateVersionFailed;
use ServiceBus\EventSourcingModule\SqlSchemaCreator;
use ServiceBus\EventSourcingModule\Tests\stubs\Context;
use ServiceBus\MessageSerializer\Symfony\SymfonyMessageSerializer;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Common\StorageConfiguration;
use ServiceBus\Storage\Sql\AmpPosgreSQL\AmpPostgreSQLAdapter;

/**
 *
 */
final class EventSourcingProviderTest extends TestCase
{
    /**
     * @var DatabaseAdapter
     */
    private static $adapter;

    /**
     * @var EventStreamStore
     */
    private $eventStore;

    /**
     * @var SnapshotStore
     */
    private $snapshotStore;

    /**
     * @var Snapshotter
     */
    private $snapshotter;

    /**
     * @var EventStreamRepository
     */
    private $eventStreamRepository;

    /**
     * @var EventSourcingProvider
     */
    private $eventSourcingProvider;

    /**
     * @var TestHandler
     */
    public $testLogHandler;

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        self::$adapter = new AmpPostgreSQLAdapter(
            new StorageConfiguration((string) \getenv('TEST_POSTGRES_DSN'))
        );

        wait((new SqlSchemaCreator(self::$adapter, \realpath(__DIR__ . '/../vendor/php-service-bus/event-sourcing/')))->import());
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    public static function tearDownAfterClass(): void
    {
        parent::tearDownAfterClass();

        wait(self::$adapter->execute('DROP TABLE event_store_stream CASCADE'));
        wait(self::$adapter->execute('DROP TABLE event_store_stream_events CASCADE'));
        wait(self::$adapter->execute('DROP TABLE event_store_snapshots CASCADE'));
        wait(self::$adapter->execute('DROP TABLE event_sourcing_indexes CASCADE'));

        self::$adapter = null;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->testLogHandler        = new TestHandler();
        $this->eventStore            = new SqlEventStreamStore(self::$adapter);
        $this->snapshotStore         = new SqlSnapshotStore(self::$adapter);
        $this->snapshotter           = new Snapshotter($this->snapshotStore, new SnapshotVersionTrigger(2));
        $this->eventStreamRepository = new EventStreamRepository(
            $this->eventStore,
            $this->snapshotter,
            new DefaultEventSerializer(
                new SymfonyMessageSerializer()
            ),
            new Logger('tests', [$this->testLogHandler])
        );

        $this->eventSourcingProvider = new EventSourcingProvider($this->eventStreamRepository);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        wait(self::$adapter->execute('TRUNCATE TABLE event_store_stream CASCADE'));
        wait(self::$adapter->execute('TRUNCATE TABLE event_store_stream_events CASCADE'));
        wait(self::$adapter->execute('TRUNCATE TABLE event_store_snapshots CASCADE'));

        unset($this->eventStore, $this->snapshotStore, $this->snapshotter, $this->eventStreamRepository, $this->eventSourcingProvider, $this->testLogHandler);
    }

    /**
     * @test
     *
     * @return void
     */
    public function flow(): void
    {
        Loop::run(
            function (): \Generator
            {
                $context   = new Context();
                $aggregate = new TestAggregate(TestAggregateId::new());

                yield $this->eventSourcingProvider->save($aggregate, $context);

                static::assertCount(1, $context->messages);

                /** @var Aggregate $loadedAggregate */
                $loadedAggregate = yield $this->eventSourcingProvider->load($aggregate->id());

                static::assertNotNull($loadedAggregate);
            }
        );
    }

    /**
     * @test
     *
     * @return void
     */
    public function saveDuplicate(): void
    {
        $this->expectException(DuplicateAggregate::class);

        Loop::run(
            function (): \Generator
            {
                $id = TestAggregateId::new();

                $context = new Context();

                yield $this->eventSourcingProvider->save(new TestAggregate($id), $context);
                yield $this->eventSourcingProvider->save(new TestAggregate($id), $context);
            }
        );
    }

    /**
     * @test
     *
     * @return void
     */
    public function successHardDeleteRevert(): void
    {
        Loop::run(
            function (): \Generator
            {
                $context   = new Context();
                $aggregate = new TestAggregate(TestAggregateId::new());

                yield $this->eventSourcingProvider->save($aggregate, $context);

                foreach (\range(1, 6) as $item)
                {
                    $aggregate->firstAction($item + 1 . ' event');
                }

                /** 7 aggregate version */
                yield $this->eventSourcingProvider->save($aggregate, $context);

                /** 7 aggregate version */
                static::assertSame(7, $aggregate->version());
                static::assertSame('7 event', $aggregate->firstValue());

                /** @var TestAggregate $aggregate */
                $aggregate = yield  $this->eventSourcingProvider->revert(
                    $aggregate,
                    5,
                    EventStreamRepository::REVERT_MODE_DELETE
                );

                /** 7 aggregate version */
                static::assertSame(5, $aggregate->version());
                static::assertSame('5 event', $aggregate->firstValue());

                $eventsCount = yield  fetchOne(
                    yield self::$adapter->execute('SELECT COUNT(id) as cnt FROM event_store_stream_events')
                );

                static::assertSame(5, $eventsCount['cnt']);
            }
        );
    }

    /**
     * @test
     *
     * @return void
     */
    public function revertUnknownStream(): void
    {
        $this->expectException(RevertAggregateVersionFailed::class);

        Loop::run(
            function (): \Generator
            {
                yield $this->eventSourcingProvider->revert(
                    new TestAggregate(TestAggregateId::new()),
                    20
                );
            }
        );
    }

    /**
     * @test
     *
     * @return void
     */
    public function revertWithVersionConflict(): void
    {
        $this->expectException(RevertAggregateVersionFailed::class);

        Loop::run(
            function (): \Generator
            {
                $context   = new Context();
                $aggregate = new TestAggregate(TestAggregateId::new());

                $aggregate->firstAction('qwerty');
                $aggregate->firstAction('root');
                $aggregate->firstAction('qwertyRoot');

                yield $this->eventSourcingProvider->save($aggregate, $context);

                /** @var TestAggregate $aggregate */
                $aggregate = yield $this->eventSourcingProvider->revert($aggregate, 2);

                $aggregate->firstAction('abube');

                yield $this->eventSourcingProvider->save($aggregate, $context);
                yield $this->eventSourcingProvider->revert($aggregate, 3);
            }
        );
    }
}
