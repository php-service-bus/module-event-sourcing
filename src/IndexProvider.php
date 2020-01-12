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

use ServiceBus\Mutex\InMemory\InMemoryMutexFactory;
use function Amp\call;
use Amp\Promise;
use ServiceBus\EventSourcing\Indexes\IndexKey;
use ServiceBus\EventSourcing\Indexes\IndexValue;
use ServiceBus\EventSourcing\Indexes\Store\IndexStore;
use ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed;
use ServiceBus\Mutex\Lock;
use ServiceBus\Mutex\MutexFactory;
use ServiceBus\Storage\Common\Exceptions\UniqueConstraintViolationCheckFailed;

/**
 *
 */
final class IndexProvider
{
    /** @var IndexStore */
    private $store;

    /**
     * Mutex creator.
     *
     * @var MutexFactory
     */
    private $mutexFactory;

    /** @var Lock[] */
    private $lockCollection = [];

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
            function () use ($indexKey): \Generator
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
            }
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
            function () use ($indexKey): \Generator
            {
                try
                {
                    /** @var IndexValue|null $value */
                    $value = yield $this->store->find($indexKey);

                    return $value !== null;
                }
                catch (\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
            }
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
            function () use ($indexKey, $value): \Generator
            {
                try
                {
                    yield from $this->setupMutex($indexKey);

                    /** @var int $affectedRows */
                    $affectedRows = yield $this->store->add($indexKey, $value);

                    return $affectedRows !== 0;
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
            }
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
            function () use ($indexKey): \Generator
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
            }
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
            function () use ($indexKey, $value): \Generator
            {
                try
                {
                    yield from $this->setupMutex($indexKey);

                    /** @var int $affectedRows */
                    $affectedRows = yield $this->store->update($indexKey, $value);

                    return $affectedRows !== 0;
                }
                catch (\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
                finally
                {
                    yield from $this->releaseMutex($indexKey);
                }
            }
        );
    }

    private function setupMutex(IndexKey $indexKey): \Generator
    {
        $mutexKey = createIndexMutex($indexKey);

        if (\array_key_exists($mutexKey, $this->lockCollection) === false)
        {
            $mutex = $this->mutexFactory->create($mutexKey);

            /** @var Lock $lock */
            $lock = yield $mutex->acquire();

            $this->lockCollection[$mutexKey] = $lock;
        }
    }

    private function releaseMutex(IndexKey $indexKey): \Generator
    {
        $mutexKey = createIndexMutex($indexKey);

        if (\array_key_exists($mutexKey, $this->lockCollection) === true)
        {
            /** @var Lock $lock */
            $lock = $this->lockCollection[$mutexKey];

            unset($this->lockCollection[$mutexKey]);

            yield $lock->release();
        }
    }
}
