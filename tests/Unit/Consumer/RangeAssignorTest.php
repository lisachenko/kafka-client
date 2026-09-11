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
use Protocol\Kafka\Consumer\AbstractPartitionAssignor;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\RangeAssignor;
use Protocol\Kafka\Consumer\Subscription;

/**
 * Replays the cases of `RangeAssignorTest` of the Java client of Kafka 0.9.0.1.
 *
 * Every test below is the case of the same name of the Java suite, with the same input and the same expected
 * assignment; unlike the Java assertions, which compare sets, these compare the lists themselves, because this
 * implementation is deterministic - the members of a topic are sorted lexicographically and its partitions are
 * handed out in numeric order.
 *
 * @see docs/protocol/2.8.md, section "Consumer group protocol (protocol_type = consumer)"
 */
#[CoversClass(RangeAssignor::class)]
#[CoversClass(AbstractPartitionAssignor::class)]
final class RangeAssignorTest extends TestCase
{
    private RangeAssignor $assignor;

    protected function setUp(): void
    {
        $this->assignor = new RangeAssignor();
    }

    public function testAnnouncesTheWireNameOfTheJavaAssignor(): void
    {
        self::assertSame('range', $this->assignor->name());
        self::assertSame(RangeAssignor::NAME, $this->assignor->name());
    }

    public function testSubscribesWithTheTopicsOfTheConsumer(): void
    {
        $subscription = $this->assignor->subscription(['topic1', 'topic2']);

        self::assertSame(['topic1', 'topic2'], $subscription->topics);
        self::assertSame(Subscription::VERSION, $subscription->version);
        self::assertSame('', $subscription->userData);
    }

    public function testOneConsumerNoTopic(): void
    {
        $assignment = $this->assignor->assign([], ['consumer' => new Subscription([])]);

        self::assertSame(['consumer'], array_keys($assignment));
        self::assertSame([], $assignment['consumer']->partitions());
    }

    public function testOneConsumerNonexistentTopic(): void
    {
        $assignment = $this->assignor->assign(['topic' => 0], ['consumer' => new Subscription(['topic'])]);

        self::assertSame(['consumer'], array_keys($assignment));
        self::assertSame([], $assignment['consumer']->partitions());
    }

    public function testOneConsumerOneTopic(): void
    {
        $assignment = $this->assignor->assign(['topic' => 3], ['consumer' => new Subscription(['topic'])]);

        self::assertSame(['consumer'], array_keys($assignment));
        self::assertSame(['topic' => [0, 1, 2]], $assignment['consumer']->partitions());
    }

    public function testOnlyAssignsPartitionsFromSubscribedTopics(): void
    {
        $assignment = $this->assignor->assign(
            ['topic' => 3, 'other' => 3],
            ['consumer' => new Subscription(['topic'])]
        );

        self::assertSame(['consumer'], array_keys($assignment));
        self::assertSame(['topic' => [0, 1, 2]], $assignment['consumer']->partitions());
    }

    public function testOneConsumerMultipleTopics(): void
    {
        $assignment = $this->assignor->assign(
            ['topic1' => 1, 'topic2' => 2],
            ['consumer' => new Subscription(['topic1', 'topic2'])]
        );

        self::assertSame(['topic1' => [0], 'topic2' => [0, 1]], $assignment['consumer']->partitions());
    }

    public function testTwoConsumersOneTopicOnePartition(): void
    {
        $assignment = $this->assignor->assign(['topic' => 1], [
            'consumer1' => new Subscription(['topic']),
            'consumer2' => new Subscription(['topic']),
        ]);

        self::assertSame(['topic' => [0]], $assignment['consumer1']->partitions());
        self::assertSame([], $assignment['consumer2']->partitions());
    }

    public function testTwoConsumersOneTopicTwoPartitions(): void
    {
        $assignment = $this->assignor->assign(['topic' => 2], [
            'consumer1' => new Subscription(['topic']),
            'consumer2' => new Subscription(['topic']),
        ]);

        self::assertSame(['topic' => [0]], $assignment['consumer1']->partitions());
        self::assertSame(['topic' => [1]], $assignment['consumer2']->partitions());
    }

    public function testMultipleConsumersMixedTopics(): void
    {
        $assignment = $this->assignor->assign(['topic1' => 3, 'topic2' => 2], [
            'consumer1' => new Subscription(['topic1']),
            'consumer2' => new Subscription(['topic1', 'topic2']),
            'consumer3' => new Subscription(['topic1']),
        ]);

        self::assertSame(['topic1' => [0]], $assignment['consumer1']->partitions());
        self::assertSame(['topic1' => [1], 'topic2' => [0, 1]], $assignment['consumer2']->partitions());
        self::assertSame(['topic1' => [2]], $assignment['consumer3']->partitions());
    }

    public function testTwoConsumersTwoTopicsSixPartitions(): void
    {
        $assignment = $this->assignor->assign(['topic1' => 3, 'topic2' => 3], [
            'consumer1' => new Subscription(['topic1', 'topic2']),
            'consumer2' => new Subscription(['topic1', 'topic2']),
        ]);

        self::assertSame(['topic1' => [0, 1], 'topic2' => [0, 1]], $assignment['consumer1']->partitions());
        self::assertSame(['topic1' => [2], 'topic2' => [2]], $assignment['consumer2']->partitions());
    }

    public function testSortsTheMembersOfATopicLexicographicallyAsTheJavaAssignorDoes(): void
    {
        // The order in which the coordinator lists the members of a group is not the order of their ids: the split
        // has to be the same as the one the Java leader computes, whatever order the subscriptions arrive in
        $assignment = $this->assignor->assign(['topic' => 3], [
            'consumer-b' => new Subscription(['topic']),
            'consumer-a' => new Subscription(['topic']),
        ]);

        self::assertSame(['consumer-b', 'consumer-a'], array_keys($assignment));
        self::assertSame(['topic' => [0, 1]], $assignment['consumer-a']->partitions());
        self::assertSame(['topic' => [2]], $assignment['consumer-b']->partitions());
    }

    public function testAcceptsThePartitionIdsThemselvesInsteadOfTheirCount(): void
    {
        $assignment = $this->assignor->assign(['topic' => [2, 0, 1]], [
            'consumer1' => new Subscription(['topic']),
            'consumer2' => new Subscription(['topic']),
        ]);

        self::assertSame(['topic' => [0, 1]], $assignment['consumer1']->partitions());
        self::assertSame(['topic' => [2]], $assignment['consumer2']->partitions());
    }

    public function testReproducesTheAssignmentOfAJavaLeaderByteForByte(): void
    {
        // Two `kafka-console-consumer.sh --new-consumer` members of the group `t6-cp-range` on the topic
        // `t6-consumer-protocol` with 3 partitions; the member ids and the `member_assignment` bytes below are what
        // a Kafka 0.9.0.1 broker relayed for them in a DescribeGroups v0 response
        $topic     = 't6-consumer-protocol';
        $topicHex  = '0014' . '74362d636f6e73756d65722d70726f746f636f6c';
        $firstId   = 't6-range-1-6bfd4d6f-6861-4d42-89f9-10fb59d1ae3e';
        $secondId  = 't6-range-2-b4a0416e-6f8b-4c70-a390-a8cf638ca4da';

        $assignment = $this->assignor->assign([$topic => 3], [
            $secondId => new Subscription([$topic]),
            $firstId  => new Subscription([$topic]),
        ]);

        self::assertSame(
            '0000' . '00000001' . $topicHex . '00000002' . '00000000' . '00000001' . '00000000',
            bin2hex($assignment[$firstId]->pack())
        );
        self::assertSame(
            '0000' . '00000001' . $topicHex . '00000001' . '00000002' . '00000000',
            bin2hex($assignment[$secondId]->pack())
        );
    }

    public function testLeavesTheLastMembersOfAnOversizedGroupWithoutPartitions(): void
    {
        // Four members on a topic with 3 partitions: the Java leader of the group `t6-cp-empty` assigned one
        // partition to each of the first three members and the empty structure `000000000000000000 00` to the fourth
        $topic  = 't6-consumer-protocol';
        $member = static fn(int $index): string => "t6-empty-{$index}";

        $assignment = $this->assignor->assign([$topic => 3], [
            $member(1) => new Subscription([$topic]),
            $member(2) => new Subscription([$topic]),
            $member(3) => new Subscription([$topic]),
            $member(4) => new Subscription([$topic]),
        ]);

        self::assertSame([$topic => [0]], $assignment[$member(1)]->partitions());
        self::assertSame([$topic => [1]], $assignment[$member(2)]->partitions());
        self::assertSame([$topic => [2]], $assignment[$member(3)]->partitions());
        self::assertSame([], $assignment[$member(4)]->partitions());
        self::assertSame('0000' . '00000000' . '00000000', bin2hex($assignment[$member(4)]->pack()));
    }

    public function testTheAssignmentOfEveryMemberIsAProtocolStructure(): void
    {
        $assignment = $this->assignor->assign(['topic' => 1], [
            'consumer1' => new Subscription(['topic']),
            'consumer2' => new Subscription(['topic']),
        ]);

        self::assertContainsOnlyInstancesOf(MemberAssignment::class, $assignment);
        self::assertSame(
            '0000' . '00000001' . '0005' . '746f706963' . '00000001' . '00000000' . '00000000',
            bin2hex($assignment['consumer1']->pack())
        );
        // A member without partitions still gets a structure, with an empty topic array
        self::assertSame('0000' . '00000000' . '00000000', bin2hex($assignment['consumer2']->pack()));
    }
}
