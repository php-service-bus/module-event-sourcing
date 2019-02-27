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
use PHPUnit\Framework\Constraint\IsType;
use PHPUnit\Framework\TestCase;
use ServiceBus\EventSourcing\Indexes\IndexKey;
use ServiceBus\EventSourcing\Indexes\IndexValue;
use ServiceBus\EventSourcing\Indexes\Store\IndexStore;
use ServiceBus\EventSourcing\Indexes\Store\SqlIndexStore;
use ServiceBus\EventSourcingModule\IndexProvider;
use ServiceBus\EventSourcingModule\SqlSchemaCreator;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Common\StorageConfiguration;
use ServiceBus\Storage\Sql\AmpPosgreSQL\AmpPostgreSQLAdapter;

/**
 *
 */
final class IndexProviderTest extends TestCase
{
    /**
     * @var DatabaseAdapter
     */
    private static $adapter;

    /**
     * @var IndexStore
     */
    private $indexesStore;

    /**
     * @var IndexProvider
     */
    private $indexProvider;

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

        $this->indexesStore  = new SqlIndexStore(self::$adapter);
        $this->indexProvider = new IndexProvider($this->indexesStore);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        wait(self::$adapter->execute('TRUNCATE TABLE event_sourcing_indexes CASCADE'));

        unset($this->indexesStore, $this->indexProvider);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function save(): void
    {
        $index = IndexKey::create(__CLASS__, 'testKey');
        $value = IndexValue::create(__METHOD__);

        /** @var bool $result */
        $result = wait($this->indexProvider->add($index, $value));

        static::assertThat($result, new IsType('bool'));
        static::assertTrue($result);

        /** @var IndexValue|null $storedValue */
        $storedValue = wait($this->indexProvider->get($index));

        static::assertNotNull($storedValue);
        static::assertSame($value->value, $storedValue->value);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function saveDuplicate(): void
    {
        $index = IndexKey::create(__CLASS__, 'testKey');
        $value = IndexValue::create(__METHOD__);

        /** @var bool $result */
        $result = wait($this->indexProvider->add($index, $value));

        static::assertThat($result, new IsType('bool'));
        static::assertTrue($result);

        /** @var bool $result */
        $result = wait($this->indexProvider->add($index, $value));

        static::assertThat($result, new IsType('bool'));
        static::assertFalse($result);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function update(): void
    {
        $index = IndexKey::create(__CLASS__, 'testKey');
        $value = IndexValue::create(__METHOD__);

        wait($this->indexProvider->add($index, $value));

        $newValue = IndexValue::create('qwerty');

        wait($this->indexProvider->update($index, $newValue));

        /** @var IndexValue|null $storedValue */
        $storedValue = wait($this->indexProvider->get($index));

        static::assertNotNull($storedValue);
        static::assertSame($newValue->value, $storedValue->value);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function remove(): void
    {
        $index = IndexKey::create(__CLASS__, 'testKey');
        $value = IndexValue::create(__METHOD__);

        wait($this->indexProvider->add($index, $value));
        wait($this->indexProvider->remove($index));

        static::assertNull(wait($this->indexProvider->get($index)));
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function has(): void
    {
        $index = IndexKey::create(__CLASS__, 'testKey');
        $value = IndexValue::create(__METHOD__);

        static::assertFalse(wait($this->indexProvider->has($index)));

        wait($this->indexProvider->add($index, $value));

        static::assertTrue(wait($this->indexProvider->has($index)));
    }
}
