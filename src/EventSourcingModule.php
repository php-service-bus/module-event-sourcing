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

use ServiceBus\Common\Module\ServiceBusModule;
use ServiceBus\EventSourcing\EventStream\EventStreamRepository;
use ServiceBus\EventSourcing\EventStream\Store\EventStreamStore;
use ServiceBus\EventSourcing\EventStream\Store\SqlEventStreamStore;
use ServiceBus\EventSourcing\Indexes\Store\IndexStore;
use ServiceBus\EventSourcing\Indexes\Store\SqlIndexStore;
use ServiceBus\EventSourcing\Snapshots\Snapshotter;
use ServiceBus\EventSourcing\Snapshots\Store\SnapshotStore;
use ServiceBus\EventSourcing\Snapshots\Store\SqlSnapshotStore;
use ServiceBus\EventSourcing\Snapshots\Triggers\SnapshotTrigger;
use ServiceBus\EventSourcing\Snapshots\Triggers\SnapshotVersionTrigger;
use ServiceBus\Mutex\InMemoryMutexFactory;
use ServiceBus\Mutex\MutexFactory;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

/**
 * @todo: custom store initialization
 */
final class EventSourcingModule implements ServiceBusModule
{
    /**
     * @var string
     */
    private $eventStoreServiceId;

    /**
     * @var string
     */
    private $snapshotStoreServiceId;

    /**
     * @var string
     */
    private $indexerStore;

    /**
     * @var string|null
     */
    private $databaseAdapterServiceId;

    /**
     * @var string|null
     */
    private $customEventSerializerServiceId;

    /**
     * @var string|null
     */
    private $customSnapshotStrategyServiceId;

    /**
     * @param string $databaseAdapterServiceId
     *
     * @return self
     */
    public static function withSqlStorage(string $databaseAdapterServiceId): self
    {
        $self = new self(
            EventStreamStore::class,
            SnapshotStore::class,
            IndexStore::class
        );

        $self->databaseAdapterServiceId = $databaseAdapterServiceId;

        return $self;
    }

    /**
     * @param string $eventSerializerServiceId
     *
     * @return $this
     */
    public function withCustomEventSerializer(string $eventSerializerServiceId): self
    {
        $this->customEventSerializerServiceId = $eventSerializerServiceId;

        return $this;
    }

    /**
     * @param string $snapshotStrategyServiceId
     *
     * @return $this
     */
    public function withCustomSnapshotStrategy(string $snapshotStrategyServiceId): self
    {
        $this->customSnapshotStrategyServiceId = $snapshotStrategyServiceId;

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function boot(ContainerBuilder $containerBuilder): void
    {
        /** Default configuration used */
        if (null !== $this->databaseAdapterServiceId)
        {
            $storeArguments = [new Reference($this->databaseAdapterServiceId)];

            $containerBuilder->addDefinitions([
                $this->eventStoreServiceId    => (new Definition(SqlEventStreamStore::class))->setArguments($storeArguments),
                $this->snapshotStoreServiceId => (new Definition(SqlSnapshotStore::class))->setArguments($storeArguments),
                $this->indexerStore           => (new Definition(SqlIndexStore::class))->setArguments($storeArguments),
            ]);
        }

        $this->registerMutexFactory($containerBuilder);
        $this->registerSnapshotter($containerBuilder);
        $this->registerEventSourcingProvider($containerBuilder);
        $this->registerIndexer($containerBuilder);
    }

    /**
     * @param ContainerBuilder $containerBuilder
     */
    private function registerMutexFactory(ContainerBuilder $containerBuilder): void
    {
        if (false === $containerBuilder->hasDefinition(MutexFactory::class))
        {
            $containerBuilder->addDefinitions([
                MutexFactory::class => new Definition(InMemoryMutexFactory::class),
            ]);
        }
    }

    /**
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function registerIndexer(ContainerBuilder $containerBuilder): void
    {
        /** @psalm-suppress PossiblyNullArgument */
        $containerBuilder->addDefinitions([
            $this->indexerStore  => (new Definition(SqlIndexStore::class))->setArguments([new Reference($this->databaseAdapterServiceId)]),
            IndexProvider::class => (new Definition(IndexProvider::class))->setArguments(
                [
                    new Reference($this->indexerStore),
                    new Reference(MutexFactory::class),
                ]
            ),
        ]);
    }

    /**
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function registerEventSourcingProvider(ContainerBuilder $containerBuilder): void
    {
        $arguments = [
            new Reference($this->eventStoreServiceId),
            new Reference(Snapshotter::class),
            null !== $this->customEventSerializerServiceId
                ? new Reference($this->customEventSerializerServiceId)
                : null,
            new Reference('service_bus.logger'),
        ];

        $containerBuilder->addDefinitions([
            EventStreamRepository::class => (new Definition(EventStreamRepository::class))->setArguments($arguments),
            EventSourcingProvider::class => (new Definition(EventSourcingProvider::class))->setArguments(
                [
                    new Reference(EventStreamRepository::class),
                    new Reference(MutexFactory::class),
                ]
            ),
        ]);
    }

    /**
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function registerSnapshotter(ContainerBuilder $containerBuilder): void
    {
        if (null === $this->customSnapshotStrategyServiceId)
        {
            $containerBuilder->addDefinitions([
                SnapshotTrigger::class => new Definition(SnapshotVersionTrigger::class),
            ]);

            $this->customSnapshotStrategyServiceId = SnapshotTrigger::class;
        }

        $arguments = [
            new Reference($this->snapshotStoreServiceId),
            new Reference($this->customSnapshotStrategyServiceId),
            new Reference('service_bus.logger'),
        ];

        $containerBuilder->addDefinitions([
            Snapshotter::class => (new Definition(Snapshotter::class))->setArguments($arguments),
        ]);
    }

    /**
     * @param string $eventStoreServiceId
     * @param string $snapshotStoreServiceId
     * @param string $indexerStore
     */
    private function __construct(string $eventStoreServiceId, string $snapshotStoreServiceId, string $indexerStore)
    {
        $this->eventStoreServiceId    = $eventStoreServiceId;
        $this->snapshotStoreServiceId = $snapshotStoreServiceId;
        $this->indexerStore           = $indexerStore;
    }
}
