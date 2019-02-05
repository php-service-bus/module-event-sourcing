<?php

/**
 * Event Sourcing implementation module
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\EventSourcingModule\Tests;

use function Amp\Promise\wait;
use PHPUnit\Framework\TestCase;
use ServiceBus\EventSourcing\Aggregate;
use ServiceBus\EventSourcing\EventStream\EventStreamRepository;
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
use ServiceBus\EventSourcingModule\SqlSchemaCreator;
use ServiceBus\EventSourcingModule\Tests\stubs\Context;
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
     * @inheritdoc
     *
     * @throws \Throwable
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        self::$adapter = new AmpPostgreSQLAdapter(
            new StorageConfiguration((string) \getenv('TEST_POSTGRES_DSN'))
        );

        wait((new SqlSchemaCreator(self::$adapter))->import());
    }

    /**
     * @inheritdoc
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
     * @inheritdoc
     *
     * @throws \Throwable
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->eventStore            = new SqlEventStreamStore(self::$adapter);
        $this->snapshotStore         = new SqlSnapshotStore(self::$adapter);
        $this->snapshotter           = new Snapshotter($this->snapshotStore, new SnapshotVersionTrigger(10));
        $this->eventStreamRepository = new EventStreamRepository($this->eventStore, $this->snapshotter);
        $this->eventSourcingProvider = new EventSourcingProvider($this->eventStreamRepository);
    }

    /**
     * @inheritdoc
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        wait(self::$adapter->execute('TRUNCATE TABLE event_store_stream CASCADE'));
        wait(self::$adapter->execute('TRUNCATE TABLE event_store_stream_events CASCADE'));
        wait(self::$adapter->execute('TRUNCATE TABLE event_store_snapshots CASCADE'));

        unset($this->eventStore, $this->snapshotStore, $this->snapshotter, $this->eventStreamRepository, $this->eventSourcingProvider);
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function flow(): void
    {
        $context   = new Context();
        $aggregate = new TestAggregate(TestAggregateId::new());

        wait($this->eventSourcingProvider->save($aggregate, $context));

        static::assertCount(1, $context->messages);

        /** @var Aggregate $loadedAggregate */
        $loadedAggregate = wait($this->eventSourcingProvider->load($aggregate->id()));

        static::assertNotNull($loadedAggregate);
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function saveDuplicate(): void
    {
        $this->expectException(DuplicateAggregate::class);

        $id = TestAggregateId::new();

        $context = new Context();

        wait($this->eventSourcingProvider->save(new TestAggregate($id), $context));
        wait($this->eventSourcingProvider->save(new TestAggregate($id), $context));
    }
}
