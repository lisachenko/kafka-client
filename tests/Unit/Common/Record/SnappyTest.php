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
use Protocol\Kafka\Common\Record\Snappy;

/**
 * The snappy codec in the xerial framing that Kafka speaks.
 *
 * The framed fixture below is the Value of the wrapper message that the console producer of the 0.8.2.2 broker
 * wrote for the three messages `alpha`, `bravo` and `charlie`, i.e. output of the snappy-java encoder.
 */
#[CoversClass(Snappy::class)]
final class SnappyTest extends TestCase
{
    /**
     * Xerial-framed stream written by snappy-java, wrapping a message set of three messages
     */
    private const string BROKER_STREAM = 'glNOQVBQWQAAAAABAAAAAQAAAEVfAAAZAUwTYVflXgAA/////wAAAAVhbHBoYQ0eIAEAAAATuCxkvRkf'
        . 'EGJyYXZvDR8gAgAAABUYsZ5kFR8cB2NoYXJsaWU=';

    public function testTheStreamStartsWithTheXerialHeader(): void
    {
        self::assertSame('82534e415050590000000001' . '00000001', bin2hex(Snappy::XERIAL_HEADER));
        self::assertSame(16, strlen(Snappy::XERIAL_HEADER));
        self::assertSame(32768, Snappy::BLOCK_SIZE);

        $stream = Snappy::compress('alpha bravo charlie');

        self::assertStringStartsWith(Snappy::XERIAL_HEADER, $stream);
        self::assertTrue(Snappy::isXerialFramed($stream));
    }

    public function testTheFramedStreamIsASequenceOfLengthPrefixedBlocks(): void
    {
        $payload = str_repeat('0123456789', 8192); // 80000 bytes, three blocks of at most 32 KiB
        $stream  = Snappy::compress($payload);

        $blocks   = 0;
        $position = strlen(Snappy::XERIAL_HEADER);
        while ($position < strlen($stream)) {
            $blockLength = (int) unpack('Nlength', $stream, $position)['length'];
            $position += 4 + $blockLength;
            $blocks++;
        }

        self::assertSame(3, $blocks);
        self::assertSame(strlen($stream), $position, 'the blocks cover the stream exactly');
        self::assertSame($payload, Snappy::decompress($stream));
    }

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function payloads(): iterable
    {
        yield 'empty'                => [''];
        yield 'single byte'          => ['a'];
        yield 'short text'           => ['alpha bravo charlie'];
        yield 'repeating text'       => [str_repeat('The quick brown fox jumps over the lazy dog. ', 500)];
        yield 'long run of one byte' => [str_repeat("\x00", 70000)];
        yield 'incompressible'       => [str_repeat('incompressible-', 1) . base64_encode(random_bytes(30000))];
        yield 'binary'               => [random_bytes(40000)];
        yield 'exactly one block'    => [str_repeat('x', Snappy::BLOCK_SIZE)];
        yield 'one byte over block'  => [str_repeat('y', Snappy::BLOCK_SIZE + 1)];
    }

    #[DataProvider('payloads')]
    public function testFramedStreamsRoundTrip(string $payload): void
    {
        self::assertSame($payload, Snappy::decompress(Snappy::compress($payload)));
    }

    #[DataProvider('payloads')]
    public function testRawBlocksRoundTrip(string $payload): void
    {
        $block = Snappy::compressBlock($payload);

        self::assertFalse(Snappy::isXerialFramed($block));
        self::assertSame($payload, Snappy::decompressBlock($block));
        self::assertSame($payload, Snappy::decompress($block), 'decoding accepts a bare block as well');
    }

    public function testAStreamWrittenBySnappyJavaIsDecoded(): void
    {
        $stream = (string) base64_decode(self::BROKER_STREAM, true);

        self::assertTrue(Snappy::isXerialFramed($stream));

        $messageSet = Snappy::decompress($stream);

        self::assertSame(95, strlen($messageSet), 'the inner message set of alpha, bravo and charlie');
        self::assertStringContainsString('alpha', $messageSet);
        self::assertStringContainsString('bravo', $messageSet);
        self::assertStringContainsString('charlie', $messageSet);
    }

    public function testCompressedDataIsSmallerThanTheInputWhenItRepeats(): void
    {
        $payload = str_repeat('kafka-client ', 4000);

        self::assertLessThan(strlen($payload) / 4, strlen(Snappy::compress($payload)));
    }

    public function testATruncatedBlockLengthIsRejected(): void
    {
        $this->expectException(CorruptMessageException::class);

        Snappy::decompress(Snappy::XERIAL_HEADER . "\x00\x00");
    }

    public function testATruncatedBlockIsRejected(): void
    {
        $stream = Snappy::compress('alpha bravo charlie');

        $this->expectException(CorruptMessageException::class);

        Snappy::decompress(substr($stream, 0, -3));
    }

    public function testABlockThatDoesNotHoldWhatItAnnouncedIsRejected(): void
    {
        // Preamble that announces 10 bytes, followed by a literal element of a single byte
        $this->expectException(CorruptMessageException::class);

        Snappy::decompressBlock("\x0a\x00a");
    }

    public function testABackReferenceBeyondTheOutputIsRejected(): void
    {
        // Preamble of 5 bytes, a one byte literal and then a copy that points before the start of the output
        $this->expectException(CorruptMessageException::class);

        Snappy::decompressBlock("\x05\x00a\x05\x10");
    }
}
