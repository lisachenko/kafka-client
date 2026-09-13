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
 * The broker rejected this static consumer since another consumer with the same group.instance.id has registered with
 * a different member.id.
 *
 * Error code 82, Kafka 2.3 (KIP-345, static membership): a request that carries a `group.instance.id` (JoinGroup v5,
 * SyncGroup v3, Heartbeat v3, OffsetCommit v7, LeaveGroup v3) names an instance the coordinator knows under another
 * member id: a newer instance with the same id has replaced this one. Fatal for the fenced member.
 */
class FencedInstanceIdException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::FENCED_INSTANCE_ID, $previous);
    }
}
