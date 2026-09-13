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

namespace Protocol\Kafka\Tests\Unit\Common\Record;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\UnsupportedCompressionTypeException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Lz4;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\Snappy;

/**
 * The codecs that the Attributes byte of a message can announce in Kafka 0.10.2.2
 */
#[CoversClass(CompressionCodec::class)]
final class CompressionCodecTest extends TestCase
{
    public function testCodecsHaveTheValuesOfTheProtocol(): void
    {
        self::assertSame(0, CompressionCodec::NONE);
        self::assertSame(1, CompressionCodec::GZIP);
        self::assertSame(2, CompressionCodec::SNAPPY);
        self::assertSame(3, CompressionCodec::LZ4);
        self::assertSame(0x07, CompressionCodec::MASK);
    }

    public function testTheCodecLivesInTheThreeLowestBitsOfTheAttributes(): void
    {
        self::assertSame(CompressionCodec::NONE, CompressionCodec::fromAttributes(0b0000_0000));
        self::assertSame(CompressionCodec::GZIP, CompressionCodec::fromAttributes(0b0000_0001));
        self::assertSame(CompressionCodec::SNAPPY, CompressionCodec::fromAttributes(0b0000_0010));
        self::assertSame(CompressionCodec::LZ4, CompressionCodec::fromAttributes(0b0000_0011));
        // The timestamp type bit of message format v1 never changes the codec
        self::assertSame(CompressionCodec::SNAPPY, CompressionCodec::fromAttributes(0b0000_1010));
    }

    public function testEveryCodecOfKafkaTenIsSupported(): void
    {
        self::assertTrue(CompressionCodec::isSupported(CompressionCodec::NONE));
        self::assertTrue(CompressionCodec::isSupported(CompressionCodec::GZIP));
        self::assertTrue(CompressionCodec::isSupported(CompressionCodec::SNAPPY));
        self::assertTrue(CompressionCodec::isSupported(CompressionCodec::LZ4));
        // 4 is zstd, which arrives with Kafka 2.1 and the message format v2
        self::assertFalse(CompressionCodec::isSupported(4));
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function codecs(): iterable
    {
        yield 'none'   => [CompressionCodec::NONE];
        yield 'gzip'   => [CompressionCodec::GZIP];
        yield 'snappy' => [CompressionCodec::SNAPPY];
        yield 'lz4'    => [CompressionCodec::LZ4];
    }

    #[DataProvider('codecs')]
    public function testEveryCodecRoundTrips(int $codec): void
    {
        $payload = str_repeat('kafka message set ', 1000);

        self::assertSame($payload, CompressionCodec::decompress($codec, CompressionCodec::compress($codec, $payload)));
    }

    public function testTheUncompressedCodecPassesTheDataThrough(): void
    {
        self::assertSame('payload', CompressionCodec::compress(CompressionCodec::NONE, 'payload'));
        self::assertSame('payload', CompressionCodec::decompress(CompressionCodec::NONE, 'payload'));
    }

    public function testGzipProducesAStreamThatZlibItselfReadsBack(): void
    {
        $compressed = CompressionCodec::compress(CompressionCodec::GZIP, 'alpha bravo charlie');

        self::assertStringStartsWith("\x1f\x8b", $compressed, 'the gzip magic of RFC 1952');
        self::assertSame('alpha bravo charlie', gzdecode($compressed));
    }

    public function testSnappyProducesTheXerialFramingOfTheBroker(): void
    {
        $compressed = CompressionCodec::compress(CompressionCodec::SNAPPY, 'alpha bravo charlie');

        self::assertTrue(Snappy::isXerialFramed($compressed));
    }

    public function testABrokenGzipStreamIsReportedAsACorruptMessage(): void
    {
        $this->expectException(CorruptMessageException::class);

        CompressionCodec::decompress(CompressionCodec::GZIP, 'this is not a gzip stream');
    }

    public function testLz4ProducesTheFrameOfTheBroker(): void
    {
        $compressed = CompressionCodec::compress(CompressionCodec::LZ4, 'alpha bravo charlie');

        self::assertStringStartsWith(Lz4::MAGIC, $compressed, 'the magic number of an LZ4 frame');
        self::assertStringStartsWith(Lz4::frameDescriptor(false), $compressed);
    }

    public function testTheLz4FrameOfAMessageOfFormatV0CarriesTheBrokenDescriptorChecksum(): void
    {
        // KAFKA-3160: the clients before 0.10.0 hashed the magic number together with the frame descriptor, and the
        // broken checksum stayed the one of message format v0 so that those clients can still read the log
        $formatV0 = CompressionCodec::compress(CompressionCodec::LZ4, 'alpha bravo charlie', Message::MAGIC_V0);
        $formatV1 = CompressionCodec::compress(CompressionCodec::LZ4, 'alpha bravo charlie', Message::MAGIC_V1);

        self::assertSame(Lz4::frameDescriptor(true), substr($formatV0, 0, 7));
        self::assertSame(Lz4::frameDescriptor(false), substr($formatV1, 0, 7));
        self::assertNotSame($formatV0[6], $formatV1[6], 'only the HC byte of the two frames differs');
        self::assertSame(substr($formatV0, 7), substr($formatV1, 7));
        // Both of them are readable, whatever the message format of the message that carries them is
        self::assertSame('alpha bravo charlie', CompressionCodec::decompress(CompressionCodec::LZ4, $formatV0));
        self::assertSame('alpha bravo charlie', CompressionCodec::decompress(CompressionCodec::LZ4, $formatV1));
    }

    public function testCompressingWithAnUnknownCodecIsAConfigurationError(): void
    {
        // 4 is zstd since Kafka 2.1, so the first codec the protocol has no name for is 5
        $this->expectException(InvalidConfigurationException::class);

        CompressionCodec::compress(5, 'payload');
    }

    public function testDecompressingAnUnknownCodecIsAConfigurationError(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        CompressionCodec::decompress(5, 'payload');
    }

    public function testTheZstdCodecIsTheCompressionTypeFourOfKip110(): void
    {
        self::assertSame(4, CompressionCodec::ZSTD);
        self::assertSame(CompressionCodec::ZSTD, CompressionCodec::fromAttributes(0b0000_0100));
        self::assertSame(
            CompressionCodec::isZstdAvailable(),
            CompressionCodec::isSupported(CompressionCodec::ZSTD),
            'the codec is supported exactly when ext-zstd is loaded; there is no pure-PHP zstd'
        );
        self::assertSame(extension_loaded('zstd'), CompressionCodec::isZstdAvailable());
    }

    public function testZstdRoundTripsThroughTheExtensionOrIsRefusedWithoutIt(): void
    {
        $payload = str_repeat('a record batch of the message format v2, compressed with zstd. ', 20);

        if (!CompressionCodec::isZstdAvailable()) {
            // The client-side half of the error code 76: this build can not speak the codec of that partition
            $this->expectException(UnsupportedCompressionTypeException::class);
            $this->expectExceptionMessage('ext-zstd');

            CompressionCodec::compress(CompressionCodec::ZSTD, $payload);

            return;
        }

        $compressed = CompressionCodec::compress(CompressionCodec::ZSTD, $payload);

        self::assertNotSame($payload, $compressed);
        self::assertSame($payload, CompressionCodec::decompress(CompressionCodec::ZSTD, $compressed));
    }

    public function testDecompressingZstdWithoutTheExtensionIsTheClientSideCodeSeventySix(): void
    {
        if (CompressionCodec::isZstdAvailable()) {
            self::markTestSkipped('ext-zstd is loaded, so this build can read a zstd batch');
        }

        $this->expectException(UnsupportedCompressionTypeException::class);
        $this->expectExceptionMessage('ext-zstd is not loaded');

        CompressionCodec::decompress(CompressionCodec::ZSTD, 'whatever the broker sent');
    }
}
