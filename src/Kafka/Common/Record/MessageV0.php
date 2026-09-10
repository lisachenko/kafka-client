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
 * A single message in message format v0, the only format that Kafka 0.8 and 0.9 know.
 *
 * <pre>
 *   Message => Crc MagicByte Attributes Key Value
 *     Crc        => int32
 *     MagicByte  => int8 (0)
 *     Attributes => int8
 *     Key        => bytes
 *     Value      => bytes
 * </pre>
 *
 * There is no `Timestamp` field and no timestamp type: bit 3 of the `Attributes` byte is reserved and stays 0, and
 * the offsets of the inner messages of a compressed set are the absolute ones, not the relative ones of v1.
 *
 * A 0.10.2 broker still writes this format for a topic configured with `message.format.version=0.9.0` (or lower),
 * and it converts a stored v1 log down to it for every client that asks with a Fetch request below version 2.
 *
 * @see docs/protocol/1.1.md, section "MessageSet and Message"
 * @see kafka/message/Message.scala @ 0.10.2.2
 */
final class MessageV0 extends Message
{
    /**
     * @inheritdoc
     */
    public const int MAGIC = self::MAGIC_V0;

    /**
     * @inheritdoc
     */
    public const int MIN_SIZE = self::MIN_SIZE_V0;
}
