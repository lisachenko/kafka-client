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
use Protocol\Kafka\Common\Record\Lz4;
use Protocol\Kafka\Common\Record\Message;

/**
 * The pure-PHP lz4 codec, byte-exact against the frames of the broker and of the Java client.
 *
 * The fixture below is an LZ4 frame that the `kafka-console-producer.sh` of the 0.10.2.2 container wrote
 * (`--compression-codec lz4`), read back out of the log with a Fetch v2 request: seven messages of a compressed
 * message format v1 batch, which is the interesting case because the Java compressor emits back references that a
 * decoder has to expand correctly.
 *
 * @see docs/protocol/0.10.2.md, section "MessageSet and Message"
 */
#[CoversClass(Lz4::class)]
final class Lz4Test extends TestCase
{
    /**
     * LZ4 frame of a batch that the Java console producer of Kafka 0.10.2.2 compressed
     */
    private const string JAVA_FRAME = '04224d186040829a00000016000100f314222d0338b10100000001a084999f3effffffff0000000c6a6176612d6c696e652d'
        . '30302d0000250050227ff28f710900012e001f532e000013332e009f02000000220f987bfe2e000a13362e0010032e004f9f'
        . '27666f2e000a13392e0010042e004f11ee8ea62e00092331322e0010052e0045e66d9d5d2e001e54b8002331352e0010062e'
        . '004f98dce1e02e0006506e652d313800000000';

    /**
     * The message set that the frame above holds, a compressed batch of seven message format v1 messages
     */
    private const string JAVA_INNER_SET = '0000000000000000000000222d0338b10100000001a084999f3effffffff0000000c6a6176612d6c696e652d303000000000'
        . '00000001000000227ff28f710100000001a084999f53ffffffff0000000c6a6176612d6c696e652d30330000000000000002'
        . '000000220f987bfe0100000001a084999f53ffffffff0000000c6a6176612d6c696e652d3036000000000000000300000022'
        . '9f27666f0100000001a084999f53ffffffff0000000c6a6176612d6c696e652d303900000000000000040000002211ee8ea6'
        . '0100000001a084999f53ffffffff0000000c6a6176612d6c696e652d3132000000000000000500000022e66d9d5d01000000'
        . '01a084999f54ffffffff0000000c6a6176612d6c696e652d313500000000000000060000002298dce1e00100000001a08499'
        . '9f54ffffffff0000000c6a6176612d6c696e652d3138';

    public function testTheFrameDescriptorIsTheOneOfTheJavaClient(): void
    {
        // 04 22 4d 18 magic, FLG = version 1 + independent blocks, BD = 64 KiB blocks, HC = xxh32(FLG BD) >> 8
        self::assertSame('04224d18604082', bin2hex(Lz4::frameDescriptor(false)));
        // ... and the KAFKA-3160 variant, which hashes the magic number together with the descriptor
        self::assertSame('04224d1860401a', bin2hex(Lz4::frameDescriptor(true)));
        self::assertSame(Lz4::MAGIC, "\x04\x22\x4d\x18");
        self::assertSame(0x60, Lz4::FLG);
        self::assertSame(0x40, Lz4::BD);
    }

    public function testTheMessageFormatSelectsTheDescriptorChecksum(): void
    {
        self::assertStringStartsWith(Lz4::frameDescriptor(true), Lz4::compress('payload', Message::MAGIC_V0));
        self::assertStringStartsWith(Lz4::frameDescriptor(false), Lz4::compress('payload', Message::MAGIC_V1));
    }

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function payloads(): iterable
    {
        yield 'empty'                    => [''];
        yield 'a single byte'            => ['x'];
        yield 'shorter than a match'     => ['0123456789'];
        yield 'exactly the match limit'  => [str_repeat('a', 12)];
        yield 'one byte past the limit'  => [str_repeat('a', 13)];
        yield 'a message set'            => [str_repeat("\x00\x00\x00\x01message value ", 40)];
        yield 'highly repetitive'        => [str_repeat('a repetitive value ', 1000)];
        yield 'incompressible'           => [random_bytes(4096)];
        yield 'larger than a block'      => [str_repeat('kafka message set ', 8192)];
        yield 'incompressible and large' => [random_bytes(200000)];
    }

    #[DataProvider('payloads')]
    public function testEveryPayloadSurvivesTheFrameRoundTrip(string $payload): void
    {
        self::assertSame($payload, Lz4::decompress(Lz4::compress($payload)));
        self::assertSame($payload, Lz4::decompress(Lz4::compress($payload, Message::MAGIC_V0)));
    }

    #[DataProvider('payloads')]
    public function testEveryPayloadSurvivesTheBlockRoundTrip(string $payload): void
    {
        self::assertSame($payload, Lz4::decompressBlock(Lz4::compressBlock($payload)));
    }

    public function testAFrameEndsWithTheEndMark(): void
    {
        $frame = Lz4::compress('alpha bravo charlie');

        self::assertSame("\x00\x00\x00\x00", substr($frame, -4), 'a block length of 0 ends the frame');
    }

    public function testAnIncompressibleBlockIsStoredAsItIs(): void
    {
        $payload = random_bytes(64);

        $frame = Lz4::compress($payload);

        $blockLength = (int) unpack('Vlength', $frame, 7)['length'];
        self::assertSame(Lz4::INCOMPRESSIBLE_MASK, $blockLength & Lz4::INCOMPRESSIBLE_MASK);
        self::assertSame(64, $blockLength & ~Lz4::INCOMPRESSIBLE_MASK);
        self::assertSame($payload, substr($frame, 11, 64), 'the block is the payload itself');
        self::assertSame($payload, Lz4::decompress($frame));
    }

    public function testALargePayloadIsSplitIntoBlocksOfTheAnnouncedMaximumSize(): void
    {
        $payload = str_repeat('a repetitive value ', 20000); // 380000 bytes, six 64 KiB blocks

        $frame = Lz4::compress($payload);

        self::assertSame(65536, Lz4::blockMaximumSize(Lz4::BLOCK_SIZE_64KB));
        self::assertSame($payload, Lz4::decompress($frame));
    }

    public function testAFrameOfTheJavaClientIsDecoded(): void
    {
        $frame = (string) hex2bin(self::JAVA_FRAME);

        self::assertSame(
            self::JAVA_INNER_SET,
            bin2hex(Lz4::decompress($frame)),
            'the frame of the Java console producer holds the message set of its batch'
        );
    }

    public function testTheDecoderAcceptsBothDescriptorChecksums(): void
    {
        $correct = Lz4::compress('alpha bravo charlie', Message::MAGIC_V1);
        $broken  = Lz4::compress('alpha bravo charlie', Message::MAGIC_V0);

        self::assertNotSame($correct, $broken);
        self::assertSame('alpha bravo charlie', Lz4::decompress($correct));
        self::assertSame('alpha bravo charlie', Lz4::decompress($broken));
    }

    public function testADescriptorChecksumThatIsNeitherOfTheTwoIsRejected(): void
    {
        $frame = Lz4::compress('alpha bravo charlie');

        $this->expectException(CorruptMessageException::class);

        Lz4::decompress(substr_replace($frame, "\x00", 6, 1));
    }

    public function testAPayloadThatIsNotAnLz4FrameIsRejected(): void
    {
        $this->expectException(CorruptMessageException::class);

        Lz4::decompress('this is not an lz4 frame');
    }

    public function testAFrameWithoutItsEndMarkIsRejected(): void
    {
        $frame = Lz4::compress('alpha bravo charlie');

        $this->expectException(CorruptMessageException::class);

        Lz4::decompress(substr($frame, 0, -4));
    }

    public function testABlockThatAnnouncesMoreBytesThanTheFrameHoldsIsRejected(): void
    {
        $frame = Lz4::compress('alpha bravo charlie');

        $this->expectException(CorruptMessageException::class);

        Lz4::decompress(substr_replace($frame, pack('V', 1024), 7, 4));
    }

    public function testABackReferenceBeyondTheOutputIsRejected(): void
    {
        // A token with four literals and a match of four bytes at the distance 1000, which does not exist yet
        $block = chr((4 << 4) | 0) . 'abcd' . pack('v', 1000);

        $this->expectException(CorruptMessageException::class);

        Lz4::decompressBlock($block);
    }

    public function testAnUnsupportedBlockMaximumSizeIsRejected(): void
    {
        $this->expectException(CorruptMessageException::class);

        Lz4::blockMaximumSize(3);
    }

    public function testAFrameOfAnotherVersionIsRejected(): void
    {
        $frame = Lz4::compress('alpha bravo charlie');

        $this->expectException(CorruptMessageException::class);

        // The two highest bits of the FLG byte are the version of the frame format, which has to be 1
        Lz4::decompress(substr_replace($frame, chr(0x20), 4, 1));
    }
}
