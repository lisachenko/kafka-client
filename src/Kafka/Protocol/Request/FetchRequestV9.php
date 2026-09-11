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

namespace Protocol\Kafka\Protocol\Request;

/**
 * Fetch API (key 1), version 9
 *
 * <pre>
 *   FetchRequest (Version: 9) => ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel SessionId Epoch
 *                                [TopicName [Partition CurrentLeaderEpoch FetchOffset LogStartOffset MaxBytes]]
 *                                [TopicName [Partition]]
 * </pre>
 *
 * Version 9 (Kafka 2.1, KIP-320) is the version that gave a fetch its `current_leader_epoch`, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::$currentLeaderEpoch}. Its body is the body of
 * version 10, byte for byte - `FetchRequest.json` @ 2.8.2 has no field of version 10 - and the two differ only in
 * what the client promises about **zstd**: a version 9 fetch of a partition whose records are zstd-compressed is
 * refused with **76** `UNSUPPORTED_COMPRESSION_TYPE`, because a broker does not down-convert zstd for a client
 * that has not said it understands the codec.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v12)" and "The leader epoch (KIP-320)"
 */
final class FetchRequestV9 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
