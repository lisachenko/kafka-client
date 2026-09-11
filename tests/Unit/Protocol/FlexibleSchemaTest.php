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

namespace Protocol\Kafka\Tests\Unit\Protocol;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\PreservesUnknownTaggedFields;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV2;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\TaggedField;
use Protocol\Kafka\Tests\Fixture\BrokerRecord;
use Protocol\Kafka\Tests\Fixture\FlexibleRecord;

/**
 * Byte-exact tests for the **flexible** encoding of KIP-482 (Kafka 2.4): compact types and tagged fields.
 *
 * The one rule the whole family follows is that flexibility is a property of an api *version*, not of a field: from
 * the `flexibleVersions` of an api on, every string, byte array and array of that version announces its length as
 * an unsigned varint of `length + 1` and every structure ends in a tagged-field section. The engine therefore asks
 * the message once and hands the answer down, which is why the same {@see FlexibleRecord} scheme produces two
 * different frames here and reads back into the same values from both.
 *
 * @see docs/protocol/2.8.md, sections "Protocol primitive types" and "Implementation model"
 */
#[CoversClass(BinarySchema::class)]
#[CoversClass(TaggedField::class)]
final class FlexibleSchemaTest extends TestCase
{
    /**
     * The same object, written plainly and compactly
     *
     * plain:    00 05 "alice"  ff ff  ff ff ff ff  00 00 00 01 00 00 00 07 00 09 "localhost" 00 00 23 84
     * compact:  06 "alice"     00     00           02          00 00 00 07 0a "localhost" 00 00 23 84 00  00
     *
     * Every length prefix is one byte instead of two or four, the null string and the null byte array are both
     * `00`, the array counts `1 + 1`, the nested broker ends in its own empty tag buffer and the record ends in
     * another one.
     */
    private const string PLAIN_HEX = '0005' . '616c696365'
        . 'ffff'
        . 'ffffffff'
        . '00000001' . '00000007' . '0009' . '6c6f63616c686f7374' . '00002384';

    private const string COMPACT_HEX = '06' . '616c696365'
        . '00'
        . '00'
        . '02' . '00000007' . '0a' . '6c6f63616c686f7374' . '00002384' . '00'
        . '00';

    public function testTheSameSchemeIsWrittenPlainlyAndCompactly(): void
    {
        $record = FlexibleRecord::of('alice', null, null, [BrokerRecord::of(7, 'localhost', 9092)]);

        self::assertSame(self::PLAIN_HEX, bin2hex(self::pack($record, false)));
        self::assertSame(self::COMPACT_HEX, bin2hex(self::pack($record, true)));
    }

    public function testTheCompactFrameReadsBackIntoTheSameValues(): void
    {
        $record = FlexibleRecord::of('alice', null, null, [BrokerRecord::of(7, 'localhost', 9092)]);

        $plain   = self::unpack(self::PLAIN_HEX, false);
        $compact = self::unpack(self::COMPACT_HEX, true);

        self::assertEquals($record, $plain, 'the plain frame decodes into the record it was written from');
        self::assertEquals($plain, $compact, 'and the compact frame decodes into exactly the same object');
    }

    public function testTheAnnouncedSizeMatchesTheWrittenBytesInBothEncodings(): void
    {
        $record = FlexibleRecord::of('alice', 'al', "\x01\x02", [BrokerRecord::of(7, 'localhost', 9092)], ['x'], 42);

        foreach ([false, true] as $flexible) {
            self::assertSame(
                strlen(self::pack($record, $flexible)),
                BinarySchema::getObjectTypeSize($record, $flexible),
                'the frame size is written before the frame, so it has to be exact'
            );
        }
    }

    /**
     * A compact length is `value + 1`, and the `0` that is left over is the null of the type
     *
     * @return array<string, array{int, mixed, string}>
     */
    public static function compactTypeProvider(): array
    {
        return [
            'string'                 => [BinarySchema::TYPE_STRING, 'test', '05' . '74657374'],
            'string empty'           => [BinarySchema::TYPE_STRING, '', '01'],
            'nullable string'        => [BinarySchema::TYPE_NULLABLE_STRING, 'test', '05' . '74657374'],
            'nullable string empty'  => [BinarySchema::TYPE_NULLABLE_STRING, '', '01'],
            'nullable string null'   => [BinarySchema::TYPE_NULLABLE_STRING, null, '00'],
            'bytes'                  => [BinarySchema::TYPE_BYTEARRAY, "\xde\xad", '03' . 'dead'],
            'bytes empty'            => [BinarySchema::TYPE_BYTEARRAY, '', '01'],
            'bytes null'             => [BinarySchema::TYPE_BYTEARRAY, null, '00'],
            // A string of 127 bytes still fits into one varint byte, 128 needs two: 128 + 1 = 129 = 81 01
            'string of 127 bytes'    => [BinarySchema::TYPE_STRING, str_repeat('a', 127), '80' . '01' . str_repeat('61', 127)],
            'uuid'                   => [BinarySchema::TYPE_UUID, str_repeat("\x11", 16), str_repeat('11', 16)],
            'uuid zero'              => [BinarySchema::TYPE_UUID, str_repeat("\x00", 16), str_repeat('00', 16)],
            // The one string that keeps its int16 length inside a flexible frame
            'never compact string'   => [BinarySchema::TYPE_STRING_NEVER_COMPACT, 'test', '0004' . '74657374'],
        ];
    }

    #[DataProvider('compactTypeProvider')]
    public function testCompactTypeIsEncodedByteExact(int $type, mixed $value, string $expectedHex): void
    {
        $stream = new StringStream();
        BinarySchema::writeSingleType($type, $value, $stream, true);

        self::assertSame($expectedHex, bin2hex($stream->getBuffer()));
        self::assertSame(
            strlen($stream->getBuffer()),
            BinarySchema::getSingleTypeSize($type, $value, true),
            'the size function has to agree with the writer, byte for byte'
        );
        self::assertSame(
            $value,
            BinarySchema::readSingleType($type, new StringStream($stream->getBuffer()), '', true)
        );
    }

    /**
     * @return array<string, array{array<mixed>, array<mixed>|null, string}>
     */
    public static function compactArrayProvider(): array
    {
        return [
            'empty array' => [[BinarySchema::TYPE_INT16], [], '01'],
            'two items'   => [[BinarySchema::TYPE_INT16], [1, 2], '03' . '0001' . '0002'],
            'null array'  => [[BinarySchema::TYPE_INT16, BinarySchema::FLAG_NULLABLE => true], null, '00'],
        ];
    }

    /**
     * @param array<mixed>      $schemeType
     * @param array<mixed>|null $value
     */
    #[DataProvider('compactArrayProvider')]
    public function testCompactArrayCountsItsElementsPlusOne(array $schemeType, ?array $value, string $hex): void
    {
        $stream = new StringStream();
        BinarySchema::writeSingleType($schemeType, $value, $stream, true);

        self::assertSame($hex, bin2hex($stream->getBuffer()));
        self::assertSame(strlen($stream->getBuffer()), BinarySchema::getSingleTypeSize($schemeType, $value, true));
        self::assertSame(
            $value,
            BinarySchema::readSingleType($schemeType, new StringStream($stream->getBuffer()), '', true)
        );
    }

    public function testACompactNullOfANotNullableArrayIsRefused(): void
    {
        $this->expectException(\UnexpectedValueException::class);

        BinarySchema::readSingleType([BinarySchema::TYPE_INT16], new StringStream("\x00"), 'topics', true);
    }

    public function testACompactNullOfANotNullableStringIsRefused(): void
    {
        $this->expectException(\UnexpectedValueException::class);

        BinarySchema::readSingleType(BinarySchema::TYPE_STRING, new StringStream("\x00"), 'name', true);
    }

    public function testAUuidOfTheWrongLengthIsRefused(): void
    {
        $this->expectException(\UnexpectedValueException::class);

        BinarySchema::writeSingleType(BinarySchema::TYPE_UUID, 'too short', new StringStream(), true);
    }

    /**
     * The record batch keeps its zigzag varint counters inside a flexible frame: only the protocol types change
     */
    public function testAVarintCountedArrayIsNotAffectedByTheCompactEncoding(): void
    {
        $scheme = [BinarySchema::TYPE_STRING, BinarySchema::FLAG_VARARRAY => true];
        $stream = new StringStream();

        BinarySchema::writeSingleType($scheme, ['a'], $stream, true);

        // The count is the zigzag varint of 1 (`02`), and the string inside it is compact, because it is a field of
        // a flexible structure - a combination no frame of Kafka 2.8.2 has, and the reason this is pinned
        self::assertSame('02' . '02' . '61', bin2hex($stream->getBuffer()));
    }

    public function testAStructureWithoutTaggedFieldsStillEndsInAnEmptyTagBuffer(): void
    {
        $record = FlexibleRecord::of('a');

        self::assertStringEndsWith('00', bin2hex(self::pack($record, true)));
        self::assertSame('02' . '61' . '00' . '00' . '01' . '00', bin2hex(self::pack($record, true)));
    }

    /**
     * A tagged field is on the wire only when its value differs from the default of its declaration
     */
    public function testATaggedFieldAtItsDefaultIsLeftOut(): void
    {
        $default = FlexibleRecord::of('a', null, null, [], [], -1);
        $set     = FlexibleRecord::of('a', null, null, [], [], 0);

        self::assertStringEndsWith('00', bin2hex(self::pack($default, true)), 'no tagged field, the empty section');
        self::assertStringEndsWith(
            '01' . '01' . '08' . '0000000000000000',
            bin2hex(self::pack($set, true)),
            'one field: the tag 1, its size 8 and the int64 zero'
        );
    }

    /**
     * The tags travel in ascending order, whatever order the scheme declares them in
     */
    public function testTaggedFieldsAreWrittenInAscendingOrderOfTheirTag(): void
    {
        $record = FlexibleRecord::of('a', null, null, [], ['x'], 7);

        self::assertStringEndsWith(
            '02'                                  // two tagged fields
            . '01' . '08' . '0000000000000007'    // tag 1, eight bytes, the epoch
            . '03' . '03' . '02' . '02' . '78',   // tag 3, three bytes: a compact array of one compact string "x"
            bin2hex(self::pack($record, true))
        );
    }

    public function testATaggedFieldSurvivesTheRoundTrip(): void
    {
        $record  = FlexibleRecord::of('a', 'nick', "\x00", [], ['x', 'y'], 99);
        $decoded = self::unpack(bin2hex(self::pack($record, true)), true);

        self::assertEquals($record, $decoded);
    }

    /**
     * A tag the scheme does not know is skipped by its size - and kept, when the class has a place for it
     *
     * This is the whole point of the mechanism: a client of this release has to be able to read the frame of a
     * later one, and this package writes the unknown fields back so that a captured frame survives a round trip.
     */
    public function testAnUnknownTagIsSkippedAndPreserved(): void
    {
        // the record above, plus a tagged field 7 of two bytes between the two known ones
        $hex = '02' . '61' . '00' . '00' . '01'
            . '03'
            . '01' . '08' . '0000000000000007'
            . '07' . '02' . 'beef'
            . '09' . '01' . '2a';

        $decoded = self::unpack($hex, true);

        self::assertSame(7, $decoded->epoch, 'the known tag is decoded');
        self::assertSame([7 => "\xbe\xef", 9 => "\x2a"], $decoded->getUnknownTaggedFields());
        self::assertSame($hex, bin2hex(self::pack($decoded, true)), 'and the unknown ones are written back');
    }

    /**
     * A structure that does not keep them drops what it does not understand, as the Java client does
     */
    public function testAStructureWithoutThePreservingTraitDropsAnUnknownTag(): void
    {
        self::assertNotContains(
            PreservesUnknownTaggedFields::class,
            class_uses(BrokerRecord::class),
            'the fixture of the plain engine tests has no place for unknown tags'
        );

        $broker = BinarySchema::readObjectFromStream(
            BrokerRecord::class,
            new StringStream((string) hex2bin('00000007' . '0a' . '6c6f63616c686f7374' . '00002384' . '01' . '07' . '02' . 'beef')),
            'broker',
            true
        );

        self::assertSame(7, $broker->nodeId);
        self::assertSame('localhost', $broker->host);
        self::assertSame(
            '00000007' . '0a' . '6c6f63616c686f7374' . '00002384' . '00',
            bin2hex(self::pack($broker, true)),
            'the tag is gone and the section is empty again'
        );
    }

    /**
     * The header versions of the two message families, i.e. `ApiKeys.requestHeaderVersion()` and its response twin
     */
    public function testTheHeaderVersionFollowsTheFlexibilityOfTheMessage(): void
    {
        self::assertFalse(MetadataRequest::isFlexible(), 'Metadata is flexible from v9, and this branch sends v5');
        self::assertSame(AbstractRequest::HEADER_V1, MetadataRequest::getHeaderVersion());
        self::assertArrayNotHasKey('headerTaggedFields', MetadataRequest::getScheme());

        self::assertTrue(ApiVersionsRequest::isFlexible());
        self::assertSame(AbstractRequest::HEADER_V2, ApiVersionsRequest::getHeaderVersion());
        self::assertArrayHasKey('headerTaggedFields', ApiVersionsRequest::getScheme());
        self::assertSame(
            BinarySchema::TYPE_STRING_NEVER_COMPACT,
            ApiVersionsRequest::getScheme()['clientId'],
            'the client id of the request header v2 keeps its int16 length prefix'
        );

        self::assertFalse(ApiVersionsRequestV2::isFlexible());
        self::assertSame(AbstractRequest::HEADER_V1, ApiVersionsRequestV2::getHeaderVersion());
    }

    /**
     * The ApiVersions answer is the one flexible frame with a **v0** response header (KIP-511)
     */
    public function testTheApiVersionsResponseKeepsTheOldHeaderAlthoughItsBodyIsFlexible(): void
    {
        self::assertTrue(ApiVersionsResponse::isFlexible());
        self::assertSame(AbstractResponse::HEADER_V0, ApiVersionsResponse::getHeaderVersion());
        self::assertArrayNotHasKey('headerTaggedFields', ApiVersionsResponse::getScheme());
    }

    private static function pack(object $record, bool $flexible): string
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream($record, $stream, $flexible);

        return $stream->getBuffer();
    }

    private static function unpack(string $hex, bool $flexible): FlexibleRecord
    {
        $record = BinarySchema::readObjectFromStream(
            FlexibleRecord::class,
            new StringStream((string) hex2bin($hex)),
            'record',
            $flexible
        );
        self::assertInstanceOf(FlexibleRecord::class, $record);

        return $record;
    }
}
