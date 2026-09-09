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

/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Common\Record;

/**
 * A record in Kafka is a key-value pair with a small amount of associated metadata.
 *
 * This is the class that the producer and the consumer of every protocol line deal in; the wire format of this line
 * is {@see Message}, wrapped into a {@see MessageSet}. A record carries no headers: those arrive with the message
 * format v2 in 0.11.
 *
 * Message format v1 (Kafka 0.10.0) gave every record a `$timestamp` and the {@see TimestampType} that says where it
 * comes from: the producer stamps the `CreateTime` of a record it sends, and a topic configured with
 * `message.timestamp.type=LogAppendTime` makes the broker replace it with the time of the append. A record of a
 * message format v0 log carries no timestamp at all, which is the `null` / {@see TimestampType::NO_TIMESTAMP_TYPE}
 * pair.
 */
class Record
{
    /**
     * @param string|null $value      Contents of the record as an opaque byte array; null is a valid value on the wire
     * @param string|null $key        Optional key that the partitioner and the log compaction use
     * @param int         $attributes Attributes byte of the message that carries this record, 0 for a produced record
     * @param int|null    $offset     Offset in the log, filled in for a record that was read from a broker
     * @param int|null    $timestamp  Milliseconds since the epoch, null for a record without a timestamp
     * @param int         $timestampType One of the {@see TimestampType} constants, telling which clock stamped it
     */
    public function __construct(
        public ?string $value = null,
        public ?string $key = null,
        public int $attributes = 0,
        public ?int $offset = null,
        public ?int $timestamp = null,
        public int $timestampType = TimestampType::NO_TIMESTAMP_TYPE,
    ) {}

    /**
     * Creates a record without a key
     */
    public static function fromValue(?string $value, int $attributes = 0): static
    {
        return new static($value, null, $attributes);
    }

    /**
     * Creates a record with a key, which is what the default partitioner distributes on
     */
    public static function fromKeyValue(?string $key, ?string $value, int $attributes = 0): static
    {
        return new static($value, $key, $attributes);
    }

    /**
     * Returns the record with the given creation timestamp, the way a producer stamps it before sending it
     *
     * @param int $timestampMs Milliseconds since the epoch
     */
    public function withCreateTime(int $timestampMs): static
    {
        $stamped                = clone $this;
        $stamped->timestamp     = $timestampMs;
        $stamped->timestampType = TimestampType::CREATE_TIME;

        return $stamped;
    }
}
