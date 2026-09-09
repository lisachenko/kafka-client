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
use Protocol\Kafka\Consumer\RoundRobinAssignor;
use Protocol\Kafka\Consumer\Subscription;

/**
 * Replays the cases of `RoundRobinAssignorTest` of the Java client of Kafka 0.9.0.1.
 *
 * Every test below is the case of the same name of the Java suite, with the same input and the same expected
 * assignment - the Java suite compares the lists themselves here, because the round-robin walk is ordered: the
 * topics are sorted lexicographically, their partitions numerically, and the members lexicographically by member id.
 *
 * @see docs/protocol/0.11.0.md, section "Consumer group protocol (protocol_type = consumer)"
 */
#[CoversClass(RoundRobinAssignor::class)]
#[CoversClass(AbstractPartitionAssignor::class)]
final class RoundRobinAssignorTest extends TestCase
{
    private RoundRobinAssignor $assignor;

    protected function setUp(): void
    {
        $this->assignor = new RoundRobinAssignor();
    }

    public function testAnnouncesTheWireNameOfTheJavaAssignor(): void
    {
        self::assertSame('roundrobin', $this->assignor->name());
        self::assertSame(RoundRobinAssignor::NAME, $this->assignor->name());
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

        self::assertSame(['topic' => [0, 1, 2]], $assignment['consumer']->partitions());
    }

    public function testOnlyAssignsPartitionsFromSubscribedTopics(): void
    {
        $assignment = $this->assignor->assign(
            ['topic' => 3, 'other' => 3],
            ['consumer' => new Subscription(['topic'])]
        );

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

        self::assertSame(['topic1' => [0, 2], 'topic2' => [1]], $assignment['consumer1']->partitions());
        self::assertSame(['topic1' => [1], 'topic2' => [0, 2]], $assignment['consumer2']->partitions());
    }

    public function testWalksTheMembersInTheLexicographicOrderOfTheirIds(): void
    {
        // The subscriptions arrive in the order the coordinator lists the members in, which is not their sort order
        $assignment = $this->assignor->assign(['topic' => 3], [
            'consumer-b' => new Subscription(['topic']),
            'consumer-a' => new Subscription(['topic']),
        ]);

        self::assertSame(['consumer-b', 'consumer-a'], array_keys($assignment));
        self::assertSame(['topic' => [0, 2]], $assignment['consumer-a']->partitions());
        self::assertSame(['topic' => [1]], $assignment['consumer-b']->partitions());
    }

    public function testSortsTheTopicsBeforeWalkingTheirPartitions(): void
    {
        // The Java assignor collects the subscribed topics in a TreeSet, so the walk starts with `alpha` even when
        // the metadata and the subscription name `beta` first
        $assignment = $this->assignor->assign(['beta' => 1, 'alpha' => 1], [
            'consumer1' => new Subscription(['beta', 'alpha']),
            'consumer2' => new Subscription(['beta', 'alpha']),
        ]);

        self::assertSame(['alpha' => [0]], $assignment['consumer1']->partitions());
        self::assertSame(['beta' => [0]], $assignment['consumer2']->partitions());
    }

    public function testReproducesTheAssignmentOfAJavaLeaderByteForByte(): void
    {
        // Two `kafka-console-consumer.sh --new-consumer` members with
        // `partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor` in the group
        // `t6-cp-roundrobin` on the topic `t6-consumer-protocol` with 3 partitions; the member ids and the
        // `member_assignment` bytes below are what a Kafka 0.9.0.1 broker relayed for them
        $topic    = 't6-consumer-protocol';
        $topicHex = '0014' . '74362d636f6e73756d65722d70726f746f636f6c';
        $firstId  = 't6-rr-1-f8452934-7187-4b6e-8292-eaf4ac9f7c3f';
        $secondId = 't6-rr-2-55c6b195-e16d-4fca-8c0d-cf4ed325bd03';

        $assignment = $this->assignor->assign([$topic => 3], [
            $firstId  => new Subscription([$topic]),
            $secondId => new Subscription([$topic]),
        ]);

        self::assertSame(
            '0000' . '00000001' . $topicHex . '00000002' . '00000000' . '00000002' . '00000000',
            bin2hex($assignment[$firstId]->pack())
        );
        self::assertSame(
            '0000' . '00000001' . $topicHex . '00000001' . '00000001' . '00000000',
            bin2hex($assignment[$secondId]->pack())
        );
    }

    public function testAcceptsThePartitionIdsThemselvesInsteadOfTheirCount(): void
    {
        $assignment = $this->assignor->assign(['topic' => [2, 0, 1]], [
            'consumer1' => new Subscription(['topic']),
            'consumer2' => new Subscription(['topic']),
        ]);

        self::assertSame(['topic' => [0, 2]], $assignment['consumer1']->partitions());
        self::assertSame(['topic' => [1]], $assignment['consumer2']->partitions());
    }
}
