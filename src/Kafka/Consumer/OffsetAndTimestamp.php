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

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;

/**
 * The answer of a timestamp lookup: an offset of a partition together with the timestamp of the message it points at.
 *
 * Kafka 0.10.1 added the timestamp-based version 1 of the Offsets api (KIP-79) and, with it, the Java type of the
 * same name that {@see KafkaConsumer::offsetsForTimes()} returns. The offset is the first one whose message carries
 * a timestamp `>= t`, and the `timestamp` here is that message's own timestamp - which is `>= t` and usually not
 * equal to it.
 *
 * There is no instance of this class for a partition whose log holds no such message: a target timestamp above the
 * timestamp of every message of a partition, and any timestamp on an empty partition, are answered by the broker
 * with {@see OffsetsResponsePartition::UNKNOWN_OFFSET} and no error, which this client reports as `null`.
 */
final class OffsetAndTimestamp implements \Stringable
{
    /**
     * @param int $offset    Offset of the first message whose timestamp is at or after the one that was searched for
     * @param int $timestamp Timestamp of that message, in milliseconds since the epoch
     */
    public function __construct(
        public readonly int $offset,
        public readonly int $timestamp,
        /**
         * Epoch the leader was on when it read this offset, the `leader_epoch` of a ListOffsets v4 answer
         *
         * `null` when the answer carried none - every answer below version 4 (Kafka 2.1, KIP-320), and any
         * lookup the leader could not place - which is the `Optional<Integer> leaderEpoch()` of the Java
         * `OffsetAndTimestamp`. A consumer that seeks to this offset keeps the epoch next to the position and
         * sends it back as the `current_leader_epoch` of its next fetch.
         */
        public readonly ?int $leaderEpoch = null
    ) {}

    public function __toString(): string
    {
        return "OffsetAndTimestamp{offset={$this->offset}, timestamp={$this->timestamp}}";
    }
}
