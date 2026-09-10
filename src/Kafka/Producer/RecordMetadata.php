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

namespace Protocol\Kafka\Producer;

/**
 * The metadata for a record that has been acknowledged by the broker.
 *
 * The `$timestamp` is the timestamp the log holds for that record: the `CreateTime` that the producer stamped on the
 * first record of the batch - the record whose offset the answer reports - or, for a topic configured with
 * `message.timestamp.type=LogAppendTime`, the `LogAppendTime` that version 2 of the Produce API reports for the
 * whole batch, because the broker overwrote every timestamp of it with that value. `null` means that the batch
 * carried no timestamp at all, i.e. that it was written in message format v0.
 *
 * `$throttleTimeMs` is the `ThrottleTime` that version 1 of the Produce API added to the answer (Kafka 0.9): the
 * number of milliseconds the broker delayed the response of the batch because the client exceeded its
 * `producer_byte_rate` quota. It is 0 on a broker without quotas, and 0 for a fire-and-forget batch (`acks = 0`),
 * which is not answered at all.
 *
 * @see \Protocol\Kafka\Client::produce()
 * @see docs/protocol/1.1.md, section "Quotas and throttle time"
 */
final class RecordMetadata
{
    /**
     * @param string   $topic          Topic the records were appended to
     * @param int      $partition      Partition of that topic
     * @param int      $offset         Offset the first record of the batch was appended at, -1 with `acks = 0`
     * @param int|null $timestamp      Timestamp of the first record of the batch: the CreateTime the producer
     *                                 stamped on it, or the LogAppendTime the broker answered with
     * @param int      $throttleTimeMs Milliseconds the broker delayed this answer because of a produce quota
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $offset,
        public readonly ?int $timestamp = null,
        public readonly int $throttleTimeMs = 0,
    ) {}

    public function __toString(): string
    {
        return "{$this->topic}-{$this->partition}@{$this->offset}";
    }
}
