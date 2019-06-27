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
use ServiceBus\Common\Context\ServiceBusContext;
use ServiceBus\EventSourcing\Aggregate;
use ServiceBus\EventSourcing\AggregateId;
use ServiceBus\EventSourcing\EventStream\EventStreamRepository;
use ServiceBus\EventSourcingModule\Exceptions\DuplicateAggregate;
use ServiceBus\EventSourcingModule\Exceptions\LoadAggregateFailed;
use ServiceBus\EventSourcingModule\Exceptions\RevertAggregateVersionFailed;
use ServiceBus\EventSourcingModule\Exceptions\SaveAggregateFailed;
use ServiceBus\Mutex\InMemoryMutexFactory;
use ServiceBus\Mutex\Lock;
use ServiceBus\Mutex\MutexFactory;
use ServiceBus\Storage\Common\Exceptions\UniqueConstraintViolationCheckFailed;

/**
 *
 */
final class EventSourcingProvider
{
    /**
     * @var EventStreamRepository
     */
    private $repository;

    /**
     * List of loaded/added aggregates.
     *
     * @psalm-var array<string, string>
     *
     * @var array
     */
    private $aggregates = [];

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
     * EventSourcingProvider constructor.
     *
     * @param EventStreamRepository $repository
     * @param MutexFactory|null     $mutexFactory
     */
    public function __construct(EventStreamRepository $repository, ?MutexFactory $mutexFactory = null)
    {
        $this->repository   = $repository;
        $this->mutexFactory = $mutexFactory ?? new InMemoryMutexFactory();
    }

    /**
     * Load aggregate.
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param AggregateId $id
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\LoadAggregateFailed
     *
     * @return Promise
     *
     */
    public function load(AggregateId $id): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(AggregateId $id): \Generator
            {
                try
                {
                    yield from $this->setupMutex($id);

                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     *
                     * @var Aggregate|null $aggregate
                     */
                    $aggregate = yield $this->repository->load($id);

                    if (null !== $aggregate)
                    {
                        $this->aggregates[(string) $aggregate->id()] = \get_class($aggregate);
                    }

                    return $aggregate;
                }
                catch (\Throwable $throwable)
                {
                    yield from $this->releaseMutex($id);

                    throw LoadAggregateFailed::fromThrowable($throwable);
                }
            },
            $id
        );
    }

    /**
     * Save a new aggregate.
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param Aggregate         $aggregate
     * @param ServiceBusContext $context
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\SaveAggregateFailed
     * @throws \ServiceBus\EventSourcingModule\Exceptions\DuplicateAggregate
     *
     * @return Promise
     *
     */
    public function save(Aggregate $aggregate, ServiceBusContext $context): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(Aggregate $aggregate, ServiceBusContext $context): \Generator
            {
                try
                {
                    /** The aggregate hasn't been loaded before, which means it is new */
                    if (false === isset($this->aggregates[(string) $aggregate->id()]))
                    {
                        /**
                         * @psalm-suppress TooManyTemplateParams Wrong Promise template
                         * @psalm-var      array<int, object> $events
                         *
                         * @var object[] $events
                         */
                        $events = yield $this->repository->save($aggregate);

                        $this->aggregates[(string) $aggregate->id()] = \get_class($aggregate);
                    }
                    else
                    {
                        /**
                         * @psalm-suppress TooManyTemplateParams Wrong Promise template
                         * @psalm-var      array<int, object> $events
                         *
                         * @var object[] $events
                         */
                        $events = yield $this->repository->update($aggregate);
                    }

                    $promises = [];

                    /** @var object $event */
                    foreach ($events as $event)
                    {
                        $promises[] = $context->delivery($event);
                    }

                    yield $promises;
                }
                catch (UniqueConstraintViolationCheckFailed $exception)
                {
                    throw DuplicateAggregate::create($aggregate->id());
                }
                catch (\Throwable $throwable)
                {
                    throw SaveAggregateFailed::fromThrowable($throwable);
                }
                finally
                {
                    yield from $this->releaseMutex($aggregate->id());
                }
            },
            $aggregate,
            $context
        );
    }

    /**
     * Revert aggregate to specified version.
     *
     * Mode options:
     *   - 1 (EventStreamRepository::REVERT_MODE_SOFT_DELETE): Mark tail events as deleted (soft deletion). There may
     *   be version conflicts in some situations
     *   - 2 (EventStreamRepository::REVERT_MODE_DELETE): Removes tail events from the database (the best option)
     *
     * @noinspection   PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
     *
     * @param Aggregate $aggregate
     * @param int       $toVersion
     * @param int       $mode
     *
     * @throws \ServiceBus\EventSourcingModule\Exceptions\RevertAggregateVersionFailed
     *
     * @return Promise<\ServiceBus\EventSourcing\Aggregate>
     *
     */
    public function revert(
        Aggregate $aggregate,
        int $toVersion,
        ?int $mode = null
    ): Promise {
        $mode = $mode ?? EventStreamRepository::REVERT_MODE_SOFT_DELETE;

        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(Aggregate $aggregate, int $toVersion, int $mode): \Generator
            {
                yield from $this->setupMutex($aggregate->id());

                try
                {
                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     *
                     * @var Aggregate $aggregate
                     */
                    $aggregate = yield $this->repository->revert($aggregate, $toVersion, $mode);

                    return $aggregate;
                }
                catch (\Throwable $throwable)
                {
                    throw RevertAggregateVersionFailed::fromThrowable($throwable);
                }
                finally
                {
                    yield from $this->releaseMutex($aggregate->id());
                }
            },
            $aggregate,
            $toVersion,
            $mode
        );
    }

    /**
     * @param AggregateId $id
     *
     * @return \Generator
     */
    private function setupMutex(AggregateId $id): \Generator
    {
        $mutexKey = createAggregateMutexKey($id);

        if (false === isset($this->locks[$mutexKey]))
        {
            $mutex = $this->mutexFactory->create($mutexKey);

            /**
             * @psalm-suppress TooManyTemplateParams
             * @psalm-suppress InvalidPropertyAssignmentValue
             */
            $this->locks[$mutexKey] = yield $mutex->acquire();
        }
    }

    /**
     * @param AggregateId $id
     *
     * @return \Generator
     */
    private function releaseMutex(AggregateId $id): \Generator
    {
        $mutexKey = createAggregateMutexKey($id);

        if (true === isset($this->locks[$mutexKey]))
        {
            /** @var Lock $lock */
            $lock = $this->locks[$mutexKey];

            /** @psalm-suppress TooManyTemplateParams */
            yield $lock->release();

            unset($this->locks[$mutexKey]);
        }
    }
}
