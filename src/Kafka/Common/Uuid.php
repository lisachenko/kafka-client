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

namespace Protocol\Kafka\Common;

/**
 * The `uuid` of the protocol: 16 raw bytes, and the two ways Kafka writes them down
 *
 * **KIP-516** (Kafka 2.8) gave every topic an id that survives a delete and a re-create of the same name, and the
 * `uuid` type of the JSON message specifications carries it: a fixed-width field of **16 bytes** that no length
 * prefix precedes, which is why the compact encoding of a flexible version does not touch it
 * ({@see \Protocol\Kafka\Protocol\BinarySchema::TYPE_UUID}).
 *
 * A protocol DTO therefore holds the **raw bytes** - {@see \Protocol\Kafka\Common\TopicMetadata::$topicId} - and
 * this class is the pair of functions that turns them into the text form a human reads and back.
 *
 * **The text form is not the canonical `8-4-4-4-12` hex of RFC 4122.** `Uuid.toString()` @ 2.8.2 prints
 * `Base64.getUrlEncoder().withoutPadding()`, i.e. the 22 characters of a url-safe base64 without `=`, and
 * `Uuid.fromString()` reads exactly that - it is what `kafka-topics.sh --describe` shows as the `TopicId` of a
 * topic, so it is the form this client prints as well.
 *
 * @see docs/protocol/2.8.md, section "Topic ids (v10, KIP-516)"
 */
final class Uuid
{
    /**
     * The `Uuid.ZERO_UUID` of the Java client: 16 zero bytes, "no topic id"
     *
     * It is what a broker answers for a topic it has no id for, what a request that names topics by name sends
     * in the `TopicId` of every entry, and what a field of a version below 10 leaves behind.
     */
    public const string ZERO = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

    /**
     * Number of bytes a uuid takes on the wire
     */
    public const int SIZE = 16;

    /**
     * Formats the 16 raw bytes of a uuid the way `Uuid.toString()` @ 2.8.2 does
     *
     * @param string $bytes The 16 raw bytes as they travel on the wire
     *
     * @return string 22 characters of url-safe base64, without padding
     */
    public static function toString(string $bytes): string
    {
        if (strlen($bytes) !== self::SIZE) {
            throw new \UnexpectedValueException('A uuid is 16 raw bytes, ' . strlen($bytes) . ' given');
        }

        return rtrim(strtr(base64_encode($bytes), '+/', '-_'), '=');
    }

    /**
     * Reads the text form a broker and `kafka-topics.sh` print back into the 16 raw bytes of the wire
     *
     * @param string $uuid The url-safe base64 of {@see self::toString()}, with or without its padding
     */
    public static function fromString(string $uuid): string
    {
        $decoded = base64_decode(strtr($uuid, '-_', '+/'), true);
        if ($decoded === false || strlen($decoded) !== self::SIZE) {
            throw new \UnexpectedValueException("Not a uuid of the Kafka protocol: {$uuid}");
        }

        return $decoded;
    }

    /**
     * Tells whether these bytes are the "no topic id" of {@see self::ZERO}
     */
    public static function isZero(string $bytes): bool
    {
        return $bytes === self::ZERO;
    }
}
