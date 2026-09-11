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
 * The leader epoch in the request is older than the epoch on the broker.
 *
 * Error code 74, Kafka 2.1 (KIP-320): the `current_leader_epoch` the client sent (Fetch v9, ListOffsets v4,
 * OffsetsForLeaderEpoch v2, OffsetCommit v6) is behind the epoch the broker knows, i.e. the client works from stale
 * metadata. An `InvalidMetadataException` in the Java client, retriable after a metadata refresh.
 */
class FencedLeaderEpochException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::FENCED_LEADER_EPOCH, $previous);
    }
}
