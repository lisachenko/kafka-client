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
 * The `$timestamp` of a record is the `LogAppendTime` of version 2 of the Produce API (Kafka 0.10.0, message format
 * v1) and is therefore always `null` here: a 0.9.0.1 broker never reports one.
 *
 * `$throttleTimeMs` is the `ThrottleTime` that version 1 of the Produce API added to the answer (Kafka 0.9): the
 * number of milliseconds the broker delayed the response of the batch because the client exceeded its
 * `producer_byte_rate` quota. It is 0 on a broker without quotas, and 0 for a fire-and-forget batch (`acks = 0`),
 * which is not answered at all.
 *
 * @see \Protocol\Kafka\Client::produce()
 * @see docs/protocol/0.9.0.md, section "Quotas and throttle time"
 */
final class RecordMetadata
{
    /**
     * @param string   $topic          Topic the records were appended to
     * @param int      $partition      Partition of that topic
     * @param int      $offset         Offset the first record of the batch was appended at, -1 with `acks = 0`
     * @param int|null $timestamp      Always null in 0.9, the broker reports no `LogAppendTime`
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
