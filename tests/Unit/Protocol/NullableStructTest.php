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
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsCursor;
use Protocol\Kafka\Protocol\NullableStruct;

/**
 * The encoding of a **nullable structure**, which Kafka 3.8 brought onto the wire with the cursor of KIP-966.
 *
 * A nullable string and a nullable array announce their absence in their own length - the compact `00` of KIP-482,
 * the int32 `-1` of a plain version - so {@see BinarySchema::FLAG_NULLABLE} on the field is all they need. A
 * nullable structure has no length to borrow: `MessageDataGenerator` @ 3.9.2 writes an **int8 in front of it**,
 * `-1` for null and `1` for "a structure follows", and counts that byte in the size of the field. A present
 * structure is then the ordinary one, tagged-field section included.
 *
 * @see docs/protocol/4.3.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
#[CoversClass(NullableStruct::class)]
#[CoversClass(BinarySchema::class)]
#[CoversClass(DescribeTopicPartitionsCursor::class)]
final class NullableStructTest extends TestCase
{
    public function testANullStructureIsTheSingleByteMinusOne(): void
    {
        $type   = new NullableStruct(DescribeTopicPartitionsCursor::class);
        $stream = new StringStream('');

        BinarySchema::writeSingleType($type, null, $stream, true);

        self::assertSame('ff', bin2hex($stream->getBuffer()));
        self::assertSame(1, BinarySchema::getSingleTypeSize($type, null, true));
    }

    public function testAPresentStructureIsTheByteOneAndTheStructureItself(): void
    {
        $type   = new NullableStruct(DescribeTopicPartitionsCursor::class);
        $stream = new StringStream('');

        BinarySchema::writeSingleType($type, new DescribeTopicPartitionsCursor('t', 2), $stream, true);

        self::assertSame(
            '01'                    // the structure follows
            . '02' . '74'           // topicName = "t", compact
            . '00000002'            // partitionIndex = 2
            . '00',                 // the tagged-field section a real structure of a flexible version ends in
            bin2hex($stream->getBuffer())
        );
        self::assertSame(
            8,
            BinarySchema::getSingleTypeSize($type, new DescribeTopicPartitionsCursor('t', 2), true),
            'the announcing byte is part of the size of the field'
        );
    }

    public function testBothShapesAreReadBackAndSurviveARoundTrip(): void
    {
        $type = new NullableStruct(DescribeTopicPartitionsCursor::class);

        $absent = BinarySchema::readSingleType($type, new StringStream((string) hex2bin('ff')), 'cursor', true);
        self::assertNull($absent, 'the -1 is the null the field defaults to');

        $present = BinarySchema::readSingleType(
            $type,
            new StringStream((string) hex2bin('01' . '02' . '74' . '00000002' . '00')),
            'cursor',
            true
        );

        self::assertInstanceOf(DescribeTopicPartitionsCursor::class, $present);
        self::assertSame('t', $present->topicName);
        self::assertSame(2, $present->partitionIndex);

        $stream = new StringStream('');
        BinarySchema::writeSingleType($type, $present, $stream, true);
        self::assertSame('01' . '02' . '74' . '00000002' . '00', bin2hex($stream->getBuffer()));
    }

    /**
     * Only the byte `-1` means null: the specification writes `1` for a structure and nothing else
     */
    public function testTheAnnouncingByteIsMinusOneOrOne(): void
    {
        self::assertSame(-1, NullableStruct::ABSENT);
        self::assertSame(1, NullableStruct::PRESENT);
        self::assertSame(DescribeTopicPartitionsCursor::class, new NullableStruct(
            DescribeTopicPartitionsCursor::class
        )->type);
    }
}
