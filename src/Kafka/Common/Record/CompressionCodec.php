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

use function extension_loaded;

use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\UnsupportedCompressionTypeException;

/**
 * Compression codecs that the Attributes byte of a {@see Message} can announce.
 *
 * The three lowest bits of the Attributes byte hold the codec; bit 3 is the {@see TimestampType} of message format
 * v1 and every other bit is 0. The four codecs of Kafka 0.10.2.2 are implemented here, gzip through the zlib
 * extension of PHP and snappy and lz4 in pure PHP; **zstd** (Kafka 2.1, KIP-110) is the fifth, and it is the one
 * codec this package cannot implement itself - there is no pure-PHP zstd - so it travels through **`ext-zstd`**
 * when that extension is loaded and is refused with an {@see UnsupportedCompressionTypeException} when it is not,
 * see {@see self::ZSTD}.
 *
 * The framing of a codec is not always the one of its own specification: snappy travels in the blocking format of
 * the xerial library and lz4 in the LZ4 frame format, whose header checksum depends on the message format of the
 * message that carries it, see {@see Lz4}.
 *
 * @see docs/protocol/2.8.md, section "MessageSet and Message"
 * @see kafka/message/CompressionCodec.scala @ 0.10.2.2
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
     * The Value of the message is an LZ4 frame (the Kafka flavour of it) wrapping a complete MessageSet
     */
    public const int LZ4 = 3;

    /**
     * The Value of the message is a zstd frame (RFC 8478) wrapping a complete record batch (Kafka 2.1, KIP-110)
     *
     * zstd is the only codec of the protocol that a **version** of two apis states: a Produce request below v7 is
     * refused a zstd batch and a Fetch request below v10 is refused a zstd partition, both with **76**
     * `UNSUPPORTED_COMPRESSION_TYPE`, because a broker does **not** down-convert zstd for a client that has not
     * promised to understand it. It is also the only codec that needs an extension:
     * {@see self::isSupported()} answers `false` for it without `ext-zstd`, and compressing or decompressing it
     * then throws an {@see UnsupportedCompressionTypeException} - the class of the broker's code 76, which doubles
     * as the client-side "this build cannot speak zstd" error, see its docblock.
     *
     * It only ever appears in a **record batch of the message format v2**: the legacy message sets were frozen
     * before Kafka 2.1, and a broker refuses the codec in a magic 0 or 1 message.
     */
    public const int ZSTD = 4;

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
        return $codec === self::NONE
            || $codec === self::GZIP
            || $codec === self::SNAPPY
            || $codec === self::LZ4
            || ($codec === self::ZSTD && self::isZstdAvailable());
    }

    /**
     * Tells whether this PHP build can compress and decompress zstd, i.e. whether `ext-zstd` is loaded
     *
     * There is no pure-PHP implementation of zstd in this package and there is not going to be one: the format is
     * far too large to reimplement, and the extension is a `pecl install zstd` away. `composer.json` suggests it.
     */
    public static function isZstdAvailable(): bool
    {
        return extension_loaded('zstd');
    }

    /**
     * Compresses the payload of a wrapper message with the given codec
     *
     * @param int    $codec One of the constants of this class
     * @param string $data  Serialized message set to compress
     * @param int    $magic Message format of the wrapper message that will carry the result; it selects the frame
     *                      descriptor checksum of the lz4 codec, see {@see Lz4}
     *
     * @throws InvalidConfigurationException for a codec that this client does not implement
     */
    public static function compress(int $codec, string $data, int $magic = Message::MAGIC_V1): string
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

            case self::LZ4:
                return Lz4::compress($data, $magic);

            case self::ZSTD:
                if (!self::isZstdAvailable()) {
                    throw new UnsupportedCompressionTypeException([
                        'error' => 'The zstd codec of KIP-110 needs the ext-zstd extension, which is not loaded',
                        'codec' => self::ZSTD,
                    ]);
                }
                $compressed = zstd_compress($data);
                if ($compressed === false) {
                    throw new InvalidConfigurationException('Unable to compress the record batch with zstd');
                }

                return $compressed;
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

            case self::LZ4:
                return Lz4::decompress($data);

            case self::ZSTD:
                if (!self::isZstdAvailable()) {
                    throw new UnsupportedCompressionTypeException([
                        'error' => 'The records are zstd-compressed (KIP-110) and ext-zstd is not loaded, so this '
                            . 'client can not read them',
                        'codec' => self::ZSTD,
                    ]);
                }
                $decompressed = @zstd_uncompress($data);
                if ($decompressed === false) {
                    throw new CorruptMessageException(['error' => 'Unable to decompress the zstd payload']);
                }

                return $decompressed;
        }

        throw new InvalidConfigurationException("Unsupported compression codec {$codec} received");
    }
}
