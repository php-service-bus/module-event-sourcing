<?php

/**
 * Event Sourcing implementation module
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\EventSourcingModule\Tests\stubs;

use Amp\Promise;
use Amp\Success;
use Psr\Log\LogLevel;
use ServiceBus\Common\Context\ServiceBusContext;
use ServiceBus\Common\Endpoint\DeliveryOptions;
use function ServiceBus\Common\uuid;

/**
 *
 */
final class Context implements ServiceBusContext
{
    public $messages = [];

    /**
     * @inheritDoc
     */
    public function isValid(): bool
    {
        return true;
    }

    /**
     * @inheritDoc
     */
    public function violations(): array
    {
        return [];
    }

    /**
     * @inheritDoc
     */
    public function delivery(object $message, ?DeliveryOptions $deliveryOptions = null): Promise
    {
        $this->messages[] = $message;

        return new Success();
    }

    /**
     * @inheritDoc
     */
    public function logContextMessage(string $logMessage, array $extra = [], string $level = LogLevel::INFO): void
    {
        // TODO: Implement logContextMessage() method.
    }

    /**
     * @inheritDoc
     */
    public function logContextThrowable(\Throwable $throwable, string $level = LogLevel::ERROR, array $extra = []): void
    {
        // TODO: Implement logContextThrowable() method.
    }

    /**
     * @inheritDoc
     */
    public function operationId(): string
    {
        return uuid();
    }

    /**
     * @inheritDoc
     */
    public function traceId(): string
    {
        return uuid();
    }


}
