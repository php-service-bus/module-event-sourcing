<?php

/**
 * Event Sourcing implementation module.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\EventSourcingModule\Exceptions;

/**
 *
 */
final class SaveAggregateFailed extends \RuntimeException
{
    public static function fromThrowable(\Throwable $throwable): self
    {
        return new self(
            $throwable->getMessage(),
            (int) $throwable->getCode(),
            $throwable
        );
    }
}
