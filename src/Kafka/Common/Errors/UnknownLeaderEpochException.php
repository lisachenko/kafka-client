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
 * The leader epoch in the request is newer than the epoch on the broker.
 *
 * Error code 75, Kafka 2.1 (KIP-320): the `current_leader_epoch` the client sent is ahead of the epoch the broker
 * knows, i.e. the broker has not caught up with the election the client already saw. Retriable in the Java client: the
 * broker will learn the new epoch.
 */
class UnknownLeaderEpochException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNKNOWN_LEADER_EPOCH, $previous);
    }
}
