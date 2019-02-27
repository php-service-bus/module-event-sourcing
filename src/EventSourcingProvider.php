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
     * @param EventStreamRepository $repository
     */
    public function __construct(EventStreamRepository $repository)
    {
        $this->repository = $repository;
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
     */
    public function load(AggregateId $id): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(AggregateId $id): \Generator
            {
                try
                {
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
     * @throws \ServiceBus\EventSourcingModule\Exceptions\DuplicateAggregate
     * @throws \ServiceBus\EventSourcingModule\Exceptions\SaveAggregateFailed
     *
     * @return Promise
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
     */
    public function revert(
        Aggregate $aggregate,
        int $toVersion,
        int $mode = EventStreamRepository::REVERT_MODE_SOFT_DELETE
    ): Promise {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(Aggregate $aggregate, int $toVersion, int $mode): \Generator
            {
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
            },
            $aggregate,
            $toVersion,
            $mode
        );
    }
}
