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

use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;

/**
 * Compression codecs that the Attributes byte of a {@see Message} can announce.
 *
 * The three lowest bits of the Attributes byte hold the codec, every other bit is 0 in message format v0.
 * The 0.9.0.1 broker already knows the LZ4 codec (3), added for the new Java producer, but this client neither
 * produces nor consumes it.
 *
 * @see docs/protocol/0.10.2.md, section "MessageSet and Message"
 * @see kafka/message/CompressionCodec.scala @ 0.9.0.1
 */
final class CompressionCodec
{
    /**
     * The message is not compressed, its Value is the payload as it was produced
     */
    public const int NONE = 0;

    /**
     * The Value of the message is a gzip stream (RFC 1952) wrapping a complete MessageSet
     */
    public const int GZIP = 1;

    /**
     * The Value of the message is an xerial-framed snappy stream wrapping a complete MessageSet
     */
    public const int SNAPPY = 2;

    /**
     * Mask of the Attributes bits that hold the codec (bits 0-2)
     */
    public const int MASK = 0x07;

    /**
     * Compression level for the gzip codec, -1 being the default level of zlib and of the broker
     */
    private const int GZIP_LEVEL = -1;

    /**
     * This class is only a namespace for the codec constants and is never instantiated
     */
    private function __construct() {}

    /**
     * Extracts the compression codec out of the Attributes byte of a message
     */
    public static function fromAttributes(int $attributes): int
    {
        return $attributes & self::MASK;
    }

    /**
     * Tells whether this client is able to compress and to decompress the given codec
     */
    public static function isSupported(int $codec): bool
    {
        return $codec === self::NONE || $codec === self::GZIP || $codec === self::SNAPPY;
    }

    /**
     * Compresses the payload of a wrapper message with the given codec
     *
     * @throws InvalidConfigurationException for a codec that this client does not implement
     */
    public static function compress(int $codec, string $data): string
    {
        switch ($codec) {
            case self::NONE:
                return $data;

            case self::GZIP:
                $compressed = gzencode($data, self::GZIP_LEVEL);
                if ($compressed === false) {
                    throw new InvalidConfigurationException('Unable to compress the message set with gzip');
                }

                return $compressed;

            case self::SNAPPY:
                return Snappy::compress($data);
        }

        throw new InvalidConfigurationException("Unsupported compression codec {$codec} requested");
    }

    /**
     * Decompresses the payload of a wrapper message that announced the given codec
     *
     * @throws CorruptMessageException       when the compressed stream can not be decoded
     * @throws InvalidConfigurationException for a codec that this client does not implement
     */
    public static function decompress(int $codec, string $data): string
    {
        switch ($codec) {
            case self::NONE:
                return $data;

            case self::GZIP:
                $decompressed = @gzdecode($data);
                if ($decompressed === false) {
                    throw new CorruptMessageException(['error' => 'Unable to decompress the gzip payload']);
                }

                return $decompressed;

            case self::SNAPPY:
                return Snappy::decompress($data);
        }

        throw new InvalidConfigurationException("Unsupported compression codec {$codec} received");
    }
}
