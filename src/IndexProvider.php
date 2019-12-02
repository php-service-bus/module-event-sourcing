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
use ServiceBus\EventSourcing\Indexes\IndexKey;
use ServiceBus\EventSourcing\Indexes\IndexValue;
use ServiceBus\EventSourcing\Indexes\Store\IndexStore;
use ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed;
use ServiceBus\Mutex\InMemoryMutexFactory;
use ServiceBus\Mutex\Lock;
use ServiceBus\Mutex\MutexFactory;
use ServiceBus\Storage\Common\Exceptions\UniqueConstraintViolationCheckFailed;

/**
 *
 */
final class IndexProvider
{
    private IndexStore $store;

    /**
     * Current locks collection.
     *
     * @psalm-var array<string, \ServiceBus\Mutex\Lock>
     *
     * @var Lock[]
     */
    private array $locks = [];

    /**
     * Mutex creator.
     *
     * @var MutexFactory
     */
    private MutexFactory $mutexFactory;

    public function __construct(IndexStore $store, ?MutexFactory $mutexFactory = null)
    {
        $this->store        = $store;
        $this->mutexFactory = $mutexFactory ?? new InMemoryMutexFactory();
    }

    /**
     * Receive index value.
     *
     * Returns \ServiceBus\EventSourcing\Indexes\IndexValue|null
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function get(IndexKey $indexKey): Promise
    {
        return call(
            function (IndexKey $indexKey): \Generator
            {
                try
                {
                    yield from $this->setupMutex($indexKey);

                    /** @var IndexValue|null $value */
                    $value = yield $this->store->find($indexKey);

                    return $value;
                }
                catch (\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
                finally
                {
                    yield from $this->releaseMutex($indexKey);
                }
            },
            $indexKey
        );
    }

    /**
     * Is there a value in the index.
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function has(IndexKey $indexKey): Promise
    {
        return call(
            function (IndexKey $indexKey): \Generator
            {
                try
                {
                    /** @var IndexValue|null $value */
                    $value = yield $this->store->find($indexKey);

                    return null !== $value;
                }
                catch (\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
            },
            $indexKey
        );
    }

    /**
     * Add a value to the index. If false, then the value already exists.
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function add(IndexKey $indexKey, IndexValue $value): Promise
    {
        return call(
            function (IndexKey $indexKey, IndexValue $value): \Generator
            {
                try
                {
                    yield from $this->setupMutex($indexKey);

                    /** @var int $affectedRows */
                    $affectedRows = yield $this->store->add($indexKey, $value);

                    return 0 !== $affectedRows;
                }
                catch (UniqueConstraintViolationCheckFailed $exception)
                {
                    return false;
                }
                catch (\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
                finally
                {
                    yield from $this->releaseMutex($indexKey);
                }
            },
            $indexKey,
            $value
        );
    }

    /**
     * Remove value from index.
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function remove(IndexKey $indexKey): Promise
    {
        return call(
            function (IndexKey $indexKey): \Generator
            {
                try
                {
                    yield from $this->setupMutex($indexKey);
                    yield $this->store->delete($indexKey);
                }
                catch (\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
                finally
                {
                    yield from $this->releaseMutex($indexKey);
                }
            },
            $indexKey
        );
    }

    /**
     * Update value in index.
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function update(IndexKey $indexKey, IndexValue $value): Promise
    {
        return call(
            function (IndexKey $indexKey, IndexValue $value): \Generator
            {
                try
                {
                    yield from $this->setupMutex($indexKey);

                    /** @var int $affectedRows */
                    $affectedRows = yield $this->store->update($indexKey, $value);

                    return 0 !== $affectedRows;
                }
                catch (\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
                finally
                {
                    yield from $this->releaseMutex($indexKey);
                }
            },
            $indexKey,
            $value
        );
    }

    private function setupMutex(IndexKey $indexKey): \Generator
    {
        $mutexKey = createIndexMutex($indexKey);
        $mutex    = $this->mutexFactory->create($mutexKey);

        /** @psalm-suppress InvalidPropertyAssignmentValue */
        $this->locks[$mutexKey] = yield $mutex->acquire();
    }

    private function releaseMutex(IndexKey $indexKey): \Generator
    {
        $mutexKey = createIndexMutex($indexKey);

        if (true === isset($this->locks[$mutexKey]))
        {
            /** @var Lock $lock */
            $lock = $this->locks[$mutexKey];

            yield $lock->release();

            unset($this->locks[$mutexKey]);
        }
    }
}
