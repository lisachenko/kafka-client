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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\ConsumerGroupMemberDescription;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribedGroup;
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribedGroupV0;
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribeMember;
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribeMemberV0;
use Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeRequest;
use Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeRequestV0;
use Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeResponse;
use Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeResponseV0;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequest;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequestV0;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponse;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponseV0;

/**
 * What Kafka 4.0 added to the two apis of the consumer protocol: ConsumerGroupHeartbeat v1 and ConsumerGroupDescribe v1.
 *
 * `ConsumerGroupHeartbeatRequest.json` @ 4.0.0: *"Version 1 adds SubscribedTopicRegex (KIP-848), and requires the
 * consumer to generate their own Member ID (KIP-1082)"* - the nullable string sits between `subscribed_topic_names`
 * and `server_assignor`, and the answer does not change. `ConsumerGroupDescribeResponse.json` @ 4.0.0: *"Version 1
 * adds MemberType field (KIP-1099)"* - an int8 behind the target assignment of every member, and the request does
 * not change. The frames the node answered are the vectors of `consumer-group-heartbeat.json` and
 * `consumer-group-describe.json`; this class pins the layout of the classes themselves.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupHeartbeat API (key 68, v0 and v1)"
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
#[CoversClass(ConsumerGroupHeartbeatRequest::class)]
#[CoversClass(ConsumerGroupHeartbeatRequestV0::class)]
#[CoversClass(ConsumerGroupHeartbeatResponse::class)]
#[CoversClass(ConsumerGroupHeartbeatResponseV0::class)]
#[CoversClass(ConsumerGroupDescribeRequest::class)]
#[CoversClass(ConsumerGroupDescribeRequestV0::class)]
#[CoversClass(ConsumerGroupDescribeResponse::class)]
#[CoversClass(ConsumerGroupDescribeResponseV0::class)]
#[CoversClass(ConsumerGroupDescribedGroup::class)]
#[CoversClass(ConsumerGroupDescribedGroupV0::class)]
#[CoversClass(ConsumerGroupDescribeMember::class)]
#[CoversClass(ConsumerGroupDescribeMemberV0::class)]
#[CoversClass(ConsumerGroupMemberDescription::class)]
final class Kip848VersionOneTest extends TestCase
{
    public function testTheHeartbeatOfVersionOneCarriesTheRegexBehindTheTopicNames(): void
    {
        self::assertSame(1, ConsumerGroupHeartbeatRequest::VERSION);
        self::assertSame(
            ['subscribedTopicNames', 'subscribedTopicRegex', 'serverAssignor', 'topicPartitions'],
            array_slice(array_keys(ConsumerGroupHeartbeatRequest::getScheme()), -4)
        );
        self::assertArrayNotHasKey('subscribedTopicRegex', ConsumerGroupHeartbeatRequestV0::getScheme());
        self::assertSame(
            array_keys(ConsumerGroupHeartbeatResponse::getScheme()),
            array_keys(ConsumerGroupHeartbeatResponseV0::getScheme()),
            'the answer did not change'
        );
    }

    /**
     * The regex of a join and the empty list of topic names next to it: `0e` "orders-.*" and `01` []
     */
    public function testAJoinByRegexIsPackedWithTheEmptyTopicList(): void
    {
        $join = ConsumerGroupHeartbeatRequest::forJoin('g', 'm', [], 300000, subscribedTopicRegex: 'orders-.*');

        self::assertSame('orders-.*', $join->getSubscribedTopicRegex());
        self::assertSame([], $join->getSubscribedTopicNames());
        self::assertSame(
            '00000027' . '0044' . '0001' . '00000000' . '0000' . '00'
            . '02' . '67' . '02' . '6d' . '00000000' . '00' . '00' . '000493e0'
            . '01' . '0a' . bin2hex('orders-.*') . '00' . '01' . '00',
            bin2hex((string) $join)
        );
    }

    /**
     * The empty string removes a regex, a null says "unchanged"; the two are different bytes
     */
    public function testADroppedRegexIsTheEmptyStringAndAnUnchangedOneTheNull(): void
    {
        $dropped   = ConsumerGroupHeartbeatRequest::forHeartbeat(
            'g',
            'm',
            3,
            subscribedTopicRegex: ConsumerGroupHeartbeatRequest::NO_SUBSCRIBED_TOPIC_REGEX
        );
        $unchanged = ConsumerGroupHeartbeatRequest::forHeartbeat('g', 'm', 3);

        self::assertSame('', $dropped->getSubscribedTopicRegex());
        self::assertNull($unchanged->getSubscribedTopicRegex());
        $droppedHex   = bin2hex((string) $dropped);
        $unchangedHex = bin2hex((string) $unchanged);

        self::assertSame(strlen($unchangedHex), strlen($droppedHex), 'both are one byte on the wire');
        self::assertSame(
            1,
            count(array_diff_assoc(str_split($droppedHex, 2), str_split($unchangedHex, 2))),
            'and they differ in that byte alone: 01 for the empty string, 00 for the null'
        );
    }

    /**
     * The named constructors build the class they are called on, so the keep-behind writes its own version
     */
    public function testTheKeepBehindOfVersionZeroHasNoRegexField(): void
    {
        $join = ConsumerGroupHeartbeatRequestV0::forJoin('g', 'm', ['t'], 300000, subscribedTopicRegex: 'ignored');

        self::assertInstanceOf(ConsumerGroupHeartbeatRequestV0::class, $join);
        self::assertSame(0, $join->getApiVersion());
        self::assertInstanceOf(
            ConsumerGroupHeartbeatRequestV0::class,
            ConsumerGroupHeartbeatRequestV0::forLeave('g', 'm')
        );
        self::assertStringNotContainsString(bin2hex('ignored'), bin2hex((string) $join), 'version 0 drops it');
    }

    public function testTheDescribeOfVersionOneEndsEveryMemberWithItsType(): void
    {
        self::assertSame(1, ConsumerGroupDescribeRequest::VERSION);
        self::assertSame(
            array_keys(ConsumerGroupDescribeRequest::getScheme()),
            array_keys(ConsumerGroupDescribeRequestV0::getScheme()),
            'version 1 is the request of version 0'
        );
        self::assertSame('memberType', array_key_last(ConsumerGroupDescribeMember::getScheme()));
        self::assertArrayNotHasKey('memberType', ConsumerGroupDescribeMemberV0::getScheme());
        self::assertSame(
            ['groupId' => ConsumerGroupDescribedGroupV0::class],
            ConsumerGroupDescribeResponseV0::getScheme()['groups']
        );
        self::assertSame(
            ['memberId' => ConsumerGroupDescribeMemberV0::class],
            ConsumerGroupDescribedGroupV0::getScheme()['members']
        );
        self::assertSame(
            ['memberId' => ConsumerGroupDescribeMember::class],
            ConsumerGroupDescribedGroup::getScheme()['members']
        );
    }

    /**
     * The `upgraded()` of the Java `MemberDescription`: true for the consumer protocol, false for a classic member,
     * null for the -1 an answer below version 1 leaves the field at
     */
    public function testTheMemberTypeIsReportedAsTheUpgradedFlagOfTheJavaClient(): void
    {
        $member             = new ConsumerGroupDescribeMember();
        $member->assignment = $member->targetAssignment = new \Protocol\Kafka\Protocol\Data\ConsumerGroupDescribeAssignment();

        self::assertSame(ConsumerGroupDescribeMember::MEMBER_TYPE_UNKNOWN, $member->memberType, 'the default -1');
        self::assertNull(ConsumerGroupMemberDescription::fromMember($member)->upgraded());

        $member->memberType = ConsumerGroupDescribeMember::MEMBER_TYPE_CONSUMER;
        self::assertTrue(ConsumerGroupMemberDescription::fromMember($member)->upgraded());

        $member->memberType = ConsumerGroupDescribeMember::MEMBER_TYPE_CLASSIC;
        self::assertFalse(ConsumerGroupMemberDescription::fromMember($member)->upgraded());
    }

    /**
     * A version 1 answer of one member: its type is the last byte in front of the tag buffer of the member
     */
    public function testAVersionOneAnswerIsUnpackedWithTheMemberType(): void
    {
        $string = static fn(string $value): string => sprintf('%02x', strlen($value) + 1) . bin2hex($value);
        $body   = '00000001' . '00' . '00000000' . '02'
            . '0000' . '00' . $string('g') . $string('Stable') . '00000002' . '00000002' . $string('uniform')
            . '02'
            . $string('m') . '00' . '00' . '00000002' . $string('c') . $string('/h') . '01' . $string('t.*')
            . '01' . '00' . '01' . '00'
            . '00'                                      // memberType = 0: a classic member
            . '00'
            . '80000000' . '00' . '00';
        $frame  = sprintf('%08x', strlen($body) / 2) . $body;

        $response = ConsumerGroupDescribeResponse::unpack(new StringStream((string) hex2bin($frame)));
        $member   = $response->groups['g']->members['m'];

        self::assertSame(ConsumerGroupDescribeMember::MEMBER_TYPE_CLASSIC, $member->memberType);
        self::assertSame('t.*', $member->subscribedTopicRegex);
        self::assertSame($frame, bin2hex((string) $response));
    }
}
