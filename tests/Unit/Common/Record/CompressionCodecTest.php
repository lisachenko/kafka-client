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
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Snappy;

/**
 * The codecs that the Attributes byte of a message can announce in 0.9.0.1
 */
#[CoversClass(CompressionCodec::class)]
final class CompressionCodecTest extends TestCase
{
    public function testCodecsHaveTheValuesOfTheProtocol(): void
    {
        self::assertSame(0, CompressionCodec::NONE);
        self::assertSame(1, CompressionCodec::GZIP);
        self::assertSame(2, CompressionCodec::SNAPPY);
        self::assertSame(0x07, CompressionCodec::MASK);
    }

    public function testTheCodecLivesInTheThreeLowestBitsOfTheAttributes(): void
    {
        self::assertSame(CompressionCodec::NONE, CompressionCodec::fromAttributes(0b0000_0000));
        self::assertSame(CompressionCodec::GZIP, CompressionCodec::fromAttributes(0b0000_0001));
        self::assertSame(CompressionCodec::SNAPPY, CompressionCodec::fromAttributes(0b0000_0010));
        // The timestamp type bit of the later message formats never changes the codec
        self::assertSame(CompressionCodec::SNAPPY, CompressionCodec::fromAttributes(0b0000_1010));
    }

    public function testOnlyTheThreeCodecsOfThisClientAreSupported(): void
    {
        self::assertTrue(CompressionCodec::isSupported(CompressionCodec::NONE));
        self::assertTrue(CompressionCodec::isSupported(CompressionCodec::GZIP));
        self::assertTrue(CompressionCodec::isSupported(CompressionCodec::SNAPPY));
        self::assertFalse(CompressionCodec::isSupported(3), 'lz4 is not produced or consumed by this client');
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function codecs(): iterable
    {
        yield 'none'   => [CompressionCodec::NONE];
        yield 'gzip'   => [CompressionCodec::GZIP];
        yield 'snappy' => [CompressionCodec::SNAPPY];
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

    public function testCompressingWithAnUnknownCodecIsAConfigurationError(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        CompressionCodec::compress(3, 'payload');
    }

    public function testDecompressingAnUnknownCodecIsAConfigurationError(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        CompressionCodec::decompress(3, 'payload');
    }
}
