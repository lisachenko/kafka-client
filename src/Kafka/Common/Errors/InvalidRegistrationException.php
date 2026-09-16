<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Protocol\Kafka\Common\Errors;

use Exception;

/**
 * The controller has considered the broker registration to be invalid.
 *
 * Error code 119, Kafka 3.7: BrokerRegistration (62) or ControllerRegistration (70) was refused by the controller; a KRaft code that never reaches a client.
 */
class InvalidRegistrationException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_REGISTRATION, $previous);
    }
}
