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

namespace Protocol\Kafka\Tests\Unit\Consumer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * Byte-exact tests for the assignment that the leader of a `consumer` group publishes.
 *
 * The reference bytes are the `member_assignment` values that a Kafka 0.9.0.1 broker relayed for the members of a
 * group of `kafka-console-consumer.sh --new-consumer` processes, read back with a raw DescribeGroups v0 frame.
 *
 * @see docs/protocol/0.11.0.md, section "Consumer group protocol (protocol_type = consumer)"
 */
#[CoversClass(MemberAssignment::class)]
final class MemberAssignmentTest extends TestCase
{
    /**
     * Topic of the group whose assignments were captured from the broker
     */
    private const string CAPTURED_TOPIC = 't6-consumer-protocol';

    /**
     * Hex of the topic name, an int16 length and its bytes
     */
    private const string CAPTURED_TOPIC_HEX = '0014' . '74362d636f6e73756d65722d70726f746f636f6c';

    /**
     * `member_assignment` of the first member of the range group: the partitions 0 and 1 of the topic
     */
    private const string CAPTURED_TWO_PARTITIONS = '0000'                                // Version = 0
        . '00000001' . self::CAPTURED_TOPIC_HEX                                          // one topic
        . '00000002' . '00000000' . '00000001'                                           // partitions 0 and 1
        . '00000000';                                                                    // UserData = empty bytes

    /**
     * `member_assignment` of a member that the assignor left without any partition
     */
    private const string CAPTURED_EMPTY = '0000' . '00000000' . '00000000';

    public function testPacksTheAssignmentThatTheJavaLeaderSends(): void
    {
        $assignment = new MemberAssignment([self::CAPTURED_TOPIC => [0, 1]]);

        self::assertSame(self::CAPTURED_TWO_PARTITIONS, bin2hex($assignment->pack()));
    }

    public function testUnpacksTheAssignmentOfTheJavaLeader(): void
    {
        $assignment = MemberAssignment::unpack((string) hex2bin(self::CAPTURED_TWO_PARTITIONS));

        self::assertSame(MemberAssignment::VERSION, $assignment->version);
        self::assertSame([self::CAPTURED_TOPIC => [0, 1]], $assignment->partitions());
        self::assertSame('', $assignment->userData);

        $topicPartitions = $assignment->topicPartitions[self::CAPTURED_TOPIC];
        self::assertInstanceOf(PartitionsForTopic::class, $topicPartitions);
        self::assertSame(self::CAPTURED_TOPIC, $topicPartitions->topic);
    }

    public function testAMemberWithoutPartitionsGetsAnEmptyStructureAndNotEmptyBytes(): void
    {
        $assignment = new MemberAssignment();

        self::assertSame(self::CAPTURED_EMPTY, bin2hex($assignment->pack()));
        self::assertSame([], MemberAssignment::unpack((string) hex2bin(self::CAPTURED_EMPTY))->partitions());
    }

    public function testPacksSeveralTopicsWithTheirPartitions(): void
    {
        $assignment = new MemberAssignment(['foo' => [0, 2], 'bar' => [1]]);

        self::assertSame(
            '0000'                                                  // Version = 0
            . '00000002'                                            // two topics
            . '0003' . '666f6f' . '00000002' . '00000000' . '00000002'  // "foo" => 0, 2
            . '0003' . '626172' . '00000001' . '00000001'           // "bar" => 1
            . '00000000',                                           // UserData = empty bytes
            bin2hex($assignment->pack())
        );
        self::assertSame(
            ['foo' => [0, 2], 'bar' => [1]],
            MemberAssignment::unpack($assignment->pack())->partitions()
        );
    }

    public function testAcceptsAnAlreadyBuiltTopicStructure(): void
    {
        $assignment = new MemberAssignment(['foo' => new PartitionsForTopic('foo', [3])]);

        self::assertSame(['foo' => [3]], $assignment->partitions());
        self::assertSame('0000' . '00000001' . '0003666f6f' . '00000001' . '00000003' . '00000000', bin2hex($assignment->pack()));
    }

    public function testCarriesTheUserDataOfACustomAssignor(): void
    {
        $assignment = new MemberAssignment(['foo' => [0]], userData: 'sticky');

        self::assertSame(
            '0000' . '00000001' . '0003666f6f' . '00000001' . '00000000' . '00000006' . '737469636b79',
            bin2hex($assignment->pack())
        );
        self::assertSame('sticky', MemberAssignment::unpack($assignment->pack())->userData);
    }

    public function testWritesAndReadsTheNullUserDataOfTheSpecification(): void
    {
        $assignment = new MemberAssignment(['foo' => [0]], userData: null);

        self::assertSame(
            '0000' . '00000001' . '0003666f6f' . '00000001' . '00000000' . 'ffffffff',
            bin2hex($assignment->pack())
        );
        self::assertNull(MemberAssignment::unpack($assignment->pack())->userData);
    }

    public function testKeepsAHigherVersionOfTheStructure(): void
    {
        $assignment = new MemberAssignment(['foo' => [0]], version: 100);
        $restored   = MemberAssignment::unpack($assignment->pack());

        self::assertSame(100, $restored->version);
        self::assertSame(['foo' => [0]], $restored->partitions());
    }
}
