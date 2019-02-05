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
use ServiceBus\EventSourcing\Indexes\IndexKey;
use ServiceBus\EventSourcing\Indexes\IndexValue;
use ServiceBus\EventSourcing\Indexes\Store\IndexStore;
use ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed;
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
     * @param IndexStore $store
     */
    public function __construct(IndexStore $store)
    {
        $this->store = $store;
    }

    /**
     * Receive index value
     *
     * @noinspection PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey $indexKey
     *
     * @return Promise<\ServiceBus\EventSourcing\Indexes\IndexValue|null>
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function get(IndexKey $indexKey): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey): \Generator
            {
                try
                {
                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     * @var IndexValue|null $value
                     */
                    $value = yield $this->store->find($indexKey);

                    return $value;
                }
                catch(\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
            },
            $indexKey
        );
    }

    /**
     * Is there a value in the index
     *
     * @noinspection PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey $indexKey
     *
     * @return Promise<bool>
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function has(IndexKey $indexKey): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey): \Generator
            {
                try
                {
                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     * @var IndexValue|null $value
                     */
                    $value = yield $this->store->find($indexKey);

                    return null !== $value;
                }
                catch(\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
            },
            $indexKey
        );
    }

    /**
     * Add a value to the index. If false, then the value already exists
     *
     * @noinspection PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey   $indexKey
     * @param IndexValue $value
     *
     * @return Promise<bool>
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function add(IndexKey $indexKey, IndexValue $value): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey, IndexValue $value): \Generator
            {
                try
                {
                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     * @var int $affectedRows
                     */
                    $affectedRows = yield $this->store->add($indexKey, $value);

                    return 0 !== $affectedRows;
                }
                catch(UniqueConstraintViolationCheckFailed $exception)
                {
                    return false;
                }
                catch(\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
            },
            $indexKey, $value
        );
    }

    /**
     * Remove value from index
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param IndexKey $indexKey
     *
     * @return Promise It doesn't return any result
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function remove(IndexKey $indexKey): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey): \Generator
            {
                try
                {
                    yield $this->store->delete($indexKey);
                }
                catch(\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
            },
            $indexKey
        );
    }

    /**
     * Update value in index
     *
     * @noinspection PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param IndexKey   $indexKey
     * @param IndexValue $value
     *
     * @return Promise<bool>
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\IndexOperationFailed
     */
    public function update(IndexKey $indexKey, IndexValue $value): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(IndexKey $indexKey, IndexValue $value): \Generator
            {
                try
                {
                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     * @var int $affectedRows
                     */
                    $affectedRows = yield $this->store->update($indexKey, $value);

                    return 0 !== $affectedRows;
                }
                catch(\Throwable $throwable)
                {
                    throw IndexOperationFailed::fromThrowable($throwable);
                }
            },
            $indexKey, $value
        );
    }
}
