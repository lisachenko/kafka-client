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
 * The given controller ID was not registered.
 *
 * Error code 136, Kafka 4.4: KIP-1312: the answer of an UnregisterController (94) naming a controller id the cluster metadata holds no registration for.
 */
class ControllerIdNotRegisteredException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::CONTROLLER_ID_NOT_REGISTERED, $previous);
    }
}
