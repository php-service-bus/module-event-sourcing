<?php

/**
 * Event Sourcing implementation module
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\EventSourcingModule;

use function Amp\call;
use Amp\Promise;
use ServiceBus\Storage\Common\DatabaseAdapter;

/**
 * @codeCoverageIgnore
 */
final class SqlSchemaCreator
{
    private const FIXTURES = [
        /** common fixtures */
        __DIR__ . '/../vendor/php-service-bus/event-sourcing/src/EventStream/Store/schema/extensions.sql'                => false,
        /** event streams */
        __DIR__ . '/../vendor/php-service-bus/event-sourcing/src/EventStream/Store/schema/event_store_stream.sql'        => false,
        __DIR__ . '/../vendor/php-service-bus/event-sourcing/src/EventStream/Store/schema/event_store_stream_events.sql' => false,
        __DIR__ . '/../vendor/php-service-bus/event-sourcing/src/EventStream/Store/schema/indexes.sql'                   => true,
        /** snapshots */
        __DIR__ . '/../vendor/php-service-bus/event-sourcing/src/Snapshots/Store/schema/event_store_snapshots.sql'       => false,
        __DIR__ . '/../vendor/php-service-bus/event-sourcing/src/Snapshots/Store/schema/indexes.sql'                     => true,
        /** indexer */
        __DIR__ . '/../vendor/php-service-bus/event-sourcing/src/Indexes/Store/schema/event_sourcing_indexes.sql'        => false,
    ];

    /**
     * @var DatabaseAdapter
     */
    private $adapter;

    /**
     * @param DatabaseAdapter $adapter
     */
    public function __construct(DatabaseAdapter $adapter)
    {
        $this->adapter = $adapter;
    }

    /**
     * Import fixtures
     *
     * @return Promise
     */
    public function import(): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(array $fixtures): \Generator
            {
                /**
                 * @var string $filePath
                 * @var bool   $multipleQueries
                 */
                foreach($fixtures as $filePath => $multipleQueries)
                {
                    $queries = true === $multipleQueries
                        ? \array_map('trim', \file($filePath))
                        : [(string) \file_get_contents($filePath)];

                    foreach($queries as $query)
                    {
                        if('' !== $query)
                        {
                            /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                            yield $this->adapter->execute($query);
                        }
                    }
                }
            },
            self::FIXTURES
        );
    }
}
