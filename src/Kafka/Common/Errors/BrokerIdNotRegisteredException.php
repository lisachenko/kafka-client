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
 * The given broker ID was not registered.
 *
 * Error code 102, Kafka 2.8 (KIP-631, BrokerHeartbeat): a BrokerHeartbeat (63) or an UnregisterBroker (64) of the
 * KRaft mode named a broker the controller has no registration for; controller traffic only.
 */
class BrokerIdNotRegisteredException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::BROKER_ID_NOT_REGISTERED, $previous);
    }
}
