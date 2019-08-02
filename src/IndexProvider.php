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
    /**
     * @var IndexStore
     */
    private $store;

    /**
     * Current locks collection.
     *
     * @psalm-var array<string, \ServiceBus\Mutex\Lock>
     *
     * @var Lock[]
     */
    private $locks = [];

    /**
     * Mutex creator.
     *
     * @var MutexFactory
     */
    private $mutexFactory;

    /**
     * IndexProvider constructor.
     *
     * @param IndexStore        $store
     * @param MutexFactory|null $mutexFactory
     */
    public function __construct(IndexStore $store, ?MutexFactory $mutexFactory = null)
    {
        $this->store        = $store;
        $this->mutexFactory = $mutexFactory ?? new InMemoryMutexFactory();
    }

    /**
     * Receive index value.
     *
     * @noinspection   PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey $indexKey
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     *
     * @return Promise<\ServiceBus\EventSourcing\Indexes\IndexValue|null>
     */
    public function get(IndexKey $indexKey): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey): \Generator
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
     * @noinspection   PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey $indexKey
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     *
     * @return Promise<bool>
     */
    public function has(IndexKey $indexKey): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey): \Generator
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
     * @noinspection   PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey   $indexKey
     * @param IndexValue $value
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     *
     * @return Promise<bool>
     */
    public function add(IndexKey $indexKey, IndexValue $value): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey, IndexValue $value): \Generator
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
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param IndexKey $indexKey
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     *
     * @return Promise It doesn't return any result
     */
    public function remove(IndexKey $indexKey): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey): \Generator
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
     * @noinspection   PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey   $indexKey
     * @param IndexValue $value
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     *
     * @return Promise<bool>
     */
    public function update(IndexKey $indexKey, IndexValue $value): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey, IndexValue $value): \Generator
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

    /**
     * @param IndexKey $indexKey
     *
     * @return \Generator
     */
    private function setupMutex(IndexKey $indexKey): \Generator
    {
        $mutexKey = createIndexMutex($indexKey);
        $mutex    = $this->mutexFactory->create($mutexKey);

        /** @psalm-suppress InvalidPropertyAssignmentValue */
        $this->locks[$mutexKey] = yield $mutex->acquire();
    }

    /**
     * @param IndexKey $indexKey
     *
     * @return \Generator
     */
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
