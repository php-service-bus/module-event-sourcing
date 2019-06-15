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

use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ServiceBus\EventSourcingModule\EventSourcingModule;
use ServiceBus\EventSourcingModule\EventSourcingProvider;
use ServiceBus\EventSourcingModule\IndexProvider;
use ServiceBus\MessageSerializer\Symfony\SymfonyMessageSerializer;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Common\StorageConfiguration;
use ServiceBus\Storage\Sql\DoctrineDBAL\DoctrineDBALAdapter;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

/**
 *
 */
final class EventSourcingModuleTest extends TestCase
{
    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function createSqlStore(): void
    {
        $containerBuilder = new ContainerBuilder();
        $containerBuilder->addDefinitions([
            StorageConfiguration::class           => (new Definition(StorageConfiguration::class))->setArguments(['sqlite:///:memory:']),
            DatabaseAdapter::class                => (new Definition(DoctrineDBALAdapter::class))->setArguments([new Reference(StorageConfiguration::class)]),
            'service_bus.logger'                  => new Definition(NullLogger::class),
            'service_bus.decoder.default_handler' => new Definition(SymfonyMessageSerializer::class),
        ]);

        $module = EventSourcingModule::withSqlStorage(DatabaseAdapter::class);
        $module->boot($containerBuilder);

        $containerBuilder->getDefinition(IndexProvider::class)->setPublic(true);
        $containerBuilder->getDefinition(EventSourcingProvider::class)->setPublic(true);

        $containerBuilder->compile();

        $containerBuilder->get(EventSourcingProvider::class);
        $containerBuilder->get(IndexProvider::class);
    }
}
