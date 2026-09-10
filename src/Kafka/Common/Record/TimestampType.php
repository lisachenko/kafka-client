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

namespace Protocol\Kafka\Common\Record;

/**
 * Meaning of the Timestamp field that message format v1 added to a {@see Message} (Kafka 0.10.0, KIP-32).
 *
 * Bit 3 of the Attributes byte tells which clock the timestamp of a message comes from: the one of the producer that
 * created the record (`CreateTime`, the bit is 0) or the one of the broker that appended it to the log
 * (`LogAppendTime`, the bit is 1). The topic decides which of the two the log holds, with the `message.timestamp.type`
 * configuration; a producer always writes `CreateTime` and the broker rewrites the attributes when the topic asks for
 * `LogAppendTime`.
 *
 * A message of format v0 has no timestamp at all, which is what {@see TimestampType::NO_TIMESTAMP_TYPE} stands for;
 * the values are the ids of the `TimestampType` enum of the Java client.
 *
 * @see docs/protocol/1.1.md, section "MessageSet and Message"
 * @see org/apache/kafka/common/record/TimestampType.java @ 0.10.2.2
 */
final class TimestampType
{
    /**
     * The message carries no timestamp: message format v0, where the field does not exist
     */
    public const int NO_TIMESTAMP_TYPE = -1;

    /**
     * The timestamp is the one the producer stamped on the record when it created it
     */
    public const int CREATE_TIME = 0;

    /**
     * The timestamp is the one the broker stamped on the record when it appended it to the log
     */
    public const int LOG_APPEND_TIME = 1;

    /**
     * Mask of the Attributes bit that holds the timestamp type (bit 3)
     */
    public const int MASK = 0x08;

    /**
     * Position of the timestamp type inside the Attributes byte
     */
    public const int ATTRIBUTE_BIT_OFFSET = 3;

    /**
     * Names of the timestamp types, the way the broker and its tools spell them
     */
    private const array NAMES = [
        self::NO_TIMESTAMP_TYPE => 'NoTimestampType',
        self::CREATE_TIME       => 'CreateTime',
        self::LOG_APPEND_TIME   => 'LogAppendTime',
    ];

    /**
     * This class is only a namespace for the timestamp type constants and is never instantiated
     */
    private function __construct() {}

    /**
     * Extracts the timestamp type out of the Attributes byte of a message of the given magic
     *
     * @param int $attributes Attributes byte of the message
     * @param int $magic      Magic byte of the message, {@see Message::MAGIC_V0} has no timestamp at all
     */
    public static function fromAttributes(int $attributes, int $magic = Message::MAGIC_V1): int
    {
        if ($magic === Message::MAGIC_V0) {
            return self::NO_TIMESTAMP_TYPE;
        }

        return (($attributes & self::MASK) >> self::ATTRIBUTE_BIT_OFFSET) === 0
            ? self::CREATE_TIME
            : self::LOG_APPEND_TIME;
    }

    /**
     * Puts the timestamp type into the Attributes byte, leaving every other bit of it alone
     *
     * `TimestampType.updateAttributes()` of the Java client: `CreateTime` clears the bit, `LogAppendTime` sets it,
     * and a message format v0 has no such bit to begin with.
     */
    public static function updateAttributes(int $attributes, int $timestampType): int
    {
        if ($timestampType === self::LOG_APPEND_TIME) {
            return $attributes | self::MASK;
        }

        return $attributes & ~self::MASK;
    }

    /**
     * Tells whether the given value is one of the timestamp types of this protocol line
     */
    public static function isValid(int $timestampType): bool
    {
        return isset(self::NAMES[$timestampType]);
    }

    /**
     * Returns the name of a timestamp type, the way the broker spells it in `message.timestamp.type`
     */
    public static function name(int $timestampType): string
    {
        return self::NAMES[$timestampType] ?? "Unknown timestamp type {$timestampType}";
    }
}
