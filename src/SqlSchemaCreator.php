<?php

/**
 * Event Sourcing implementation module.
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
        '/src/EventStream/Store/schema/extensions.sql'                => false,
        /** event streams */
        '/src/EventStream/Store/schema/event_store_stream.sql'        => false,
        '/src/EventStream/Store/schema/event_store_stream_events.sql' => false,
        '/src/EventStream/Store/schema/indexes.sql'                   => true,
        /** snapshots */
        '/src/Snapshots/Store/schema/event_store_snapshots.sql'       => false,
        '/src/Snapshots/Store/schema/indexes.sql'                     => true,
        /** indexer */
        '/src/Indexes/Store/schema/event_sourcing_indexes.sql'        => false,
    ];

    /** @var DatabaseAdapter */
    private $adapter;

    /** @var string */
    private $rootDirectoryPath;

    public function __construct(DatabaseAdapter $adapter, string $rootDirectoryPath)
    {
        $this->adapter           = $adapter;
        $this->rootDirectoryPath = \rtrim($rootDirectoryPath, '/');
    }

    /**
     * Import fixtures.
     */
    public function import(): Promise
    {
        return call(
            function (array $fixtures): \Generator
            {
                /**
                 * @var string $filePath
                 * @var bool   $multipleQueries
                 */
                foreach ($fixtures as $filePath => $multipleQueries)
                {
                    $filePath = $this->rootDirectoryPath . $filePath;

                    /** @psalm-suppress InvalidScalarArgument */
                    $queries = true === $multipleQueries
                        ? \array_map('trim', (array) \file($filePath))
                        : [(string) \file_get_contents($filePath)];

                    foreach ($queries as $query)
                    {
                        if ($query !== '')
                        {
                            yield $this->adapter->execute($query);
                        }
                    }
                }
            },
            self::FIXTURES
        );
    }
}
