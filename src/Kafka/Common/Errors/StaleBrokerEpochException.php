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
 * Broker epoch has changed.
 *
 * Error code 77, Kafka 2.2 (KIP-380): the `broker_epoch` of a controller request (LeaderAndIsr v2, UpdateMetadata v5,
 * StopReplica v1, ControlledShutdown v2) does not match the epoch the broker registered in ZooKeeper with;
 * broker-to-controller traffic only.
 */
class StaleBrokerEpochException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::STALE_BROKER_EPOCH, $previous);
    }
}
