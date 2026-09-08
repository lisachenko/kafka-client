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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Consumer\RangeAssignor;
use Protocol\Kafka\Consumer\RoundRobinAssignor;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\ConsumerGroupMemberProcess;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Drives the group membership of {@see KafkaConsumer} against a real Kafka 0.9.0.1 broker.
 *
 * Everything a subscribed consumer does on its own is observed here: it joins the group on its first poll(), runs
 * the assignor as the leader of the generation, keeps its membership alive with the heartbeats of its poll loop,
 * commits with the member id and the generation it holds, rejoins when the coordinator reports a rebalance and
 * leaves the group when it is closed. The state of the group itself is read back with DescribeGroups, i.e. with
 * exactly the api `kafka-consumer-groups.sh --new-consumer --describe` uses.
 *
 * A group of two members needs two processes: the coordinator answers a JoinGroup only once every member of the
 * group has rejoined, so a second consumer in this very process would deadlock behind the blocked poll() of the
 * first one until a session timeout expires. The second member therefore runs in a child process, see
 * {@see ConsumerGroupMemberProcess}.
 *
 * @see docs/protocol/0.9.0.md, sections "Group membership protocol (keys 11 to 14)", "Consumer group protocol
 *      (protocol_type = consumer)" and "DescribeGroups API (key 15, v0)"
 */
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(ConsumerCoordinator::class)]
#[CoversClass(SubscriptionState::class)]
#[CoversClass(RangeAssignor::class)]
#[CoversClass(RoundRobinAssignor::class)]
#[CoversClass(Subscription::class)]
#[CoversClass(MemberAssignment::class)]
final class ConsumerGroupTest extends IntegrationTestCase
{
    /**
     * Client id of the consumer under test, which the coordinator uses as the prefix of its member id
     */
    private const string CLIENT_ID = 'kafka-client-t7';

    /**
     * Client id of the member that runs in a child process
     */
    private const string MEMBER_CLIENT_ID = 'kafka-client-t7-member';

    /**
     * Session timeout of the members here; `group.min.session.timeout.ms` of the container is 1000
     */
    private const int SESSION_TIMEOUT_MS = 6000;

    /**
     * Read timeout of the sockets, which has to cover a JoinGroup that waits for a whole rebalance
     */
    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * How long a poll loop keeps going before the expected state of the group is given up on, in seconds
     */
    private const float REBALANCE_TIMEOUT = 60.0;

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long to wait for a freshly created partition to start serving requests, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * Error codes of a partition that exists but is not being served by this broker yet
     *
     * @var list<int>
     */
    private const array NOT_SERVABLE_YET = [
        KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
        KafkaException::LEADER_NOT_AVAILABLE,
        KafkaException::NOT_LEADER_FOR_PARTITION,
    ];

    /**
     * Topic of the current test, created with its three partitions by {@see self::setUp()}
     */
    private string $topic;

    /**
     * Consumers the current test built, closed again when it ends
     *
     * @var list<KafkaConsumer>
     */
    private array $consumers = [];

    /**
     * Members of a group that run in a child process, stopped when the test ends
     *
     * @var list<ConsumerGroupMemberProcess>
     */
    private array $members = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t7-group');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        foreach ($this->members as $member) {
            $member->stop();
        }
        foreach ($this->consumers as $consumer) {
            $consumer->unsubscribe();
        }
        $this->members   = [];
        $this->consumers = [];

        parent::tearDown();
    }

    public function testASubscribedConsumerJoinsTheGroupAndReceivesEveryPartition(): void
    {
        $groupId = self::uniqueGroupName();
        $this->produce(0, ['p0-a', 'p0-b']);
        $this->produce(1, ['p1-a']);
        $this->produce(2, ['p2-a']);

        $consumer = $this->consumer($groupId);
        $consumer->subscribe([$this->topic]);

        self::assertSame([$this->topic], $consumer->subscription());
        self::assertSame([], $consumer->assignment(), 'the group is joined by the first poll(), not by subscribe()');

        $records = $this->pollUntil($consumer, 4);

        self::assertSame([0, 1, 2], $this->assignedPartitions($consumer), 'the only member gets every partition');
        self::assertSame(['p0-a', 'p0-b'], array_map(self::valueOf(...), $records[0] ?? []));
        self::assertSame(['p1-a'], array_map(self::valueOf(...), $records[1] ?? []));
        self::assertSame(['p2-a'], array_map(self::valueOf(...), $records[2] ?? []));

        // The coordinator describes exactly what this consumer sent it
        $description = $this->describeGroup($groupId);

        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $description->state);
        self::assertSame(ConsumerCoordinator::PROTOCOL_TYPE, $description->protocolType);
        self::assertSame(RangeAssignor::NAME, $description->protocol, 'the group agreed on the default assignor');
        self::assertCount(1, $description->members);

        $member = reset($description->members);

        self::assertSame(self::CLIENT_ID, $member->clientId);
        self::assertStringStartsWith(self::CLIENT_ID . '-', $member->memberId);
        self::assertEquals(
            new Subscription([$this->topic]),
            Subscription::unpack($member->memberMetadata),
            'the member metadata of the group is the Subscription this consumer packed'
        );
        self::assertSame(
            [$this->topic => [0, 1, 2]],
            MemberAssignment::unpack($member->memberAssignment)->partitions(),
            'and its assignment is the one the leader published for it'
        );
    }

    public function testTheCommittedOffsetsOfTheGroupArePickedUpByTheNextConsumer(): void
    {
        $groupId = self::uniqueGroupName();
        $this->produce(0, ['first', 'second']);
        $this->produce(1, ['third']);

        $consumer = $this->consumer($groupId);
        $consumer->subscribe([$this->topic]);
        $this->pollUntil($consumer, 3);
        $consumer->commitSync();

        $committed = $consumer->committed([$this->topic => [0, 1, 2]]);

        self::assertSame(2, $committed[$this->topic][0], 'the position is the offset of the next record');
        self::assertSame(1, $committed[$this->topic][1]);

        // The member leaves the group, so that the next consumer does not have to wait for its session timeout
        $consumer->close();

        $resumed = $this->consumer($groupId);
        $resumed->subscribe([$this->topic]);
        $this->pollUntil($resumed, 0, static fn(KafkaConsumer $polled): bool => $polled->assignment() !== []);

        self::assertSame([0, 1, 2], $this->assignedPartitions($resumed));
        self::assertSame(2, $resumed->position($this->topic, 0), 'it resumes where the group committed');
        self::assertSame(1, $resumed->position($this->topic, 1));
        self::assertSame([], $this->pollUntil($resumed, 0), 'and receives nothing that was consumed before');
    }

    public function testAClosedConsumerLeavesTheGroupBehindEmpty(): void
    {
        $groupId  = self::uniqueGroupName();
        $consumer = $this->consumer($groupId);
        $consumer->subscribe([$this->topic]);
        $this->pollUntil($consumer, 0, static fn(KafkaConsumer $polled): bool => $polled->assignment() !== []);

        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $this->describeGroup($groupId)->state);

        $consumer->unsubscribe();

        // The coordinator removes a group whose last member left, and reports it as Dead from then on
        $description = $this->describeGroup($groupId);

        self::assertSame(DescribeGroupResponseMetadata::STATE_DEAD, $description->state);
        self::assertSame([], $description->members);
        self::assertSame([], $consumer->subscription());
        self::assertSame([], $consumer->assignment());
    }

    /**
     * A consumer that stops polling for longer than its session timeout is dropped by the coordinator; the next
     * poll() learns that from the error code 25 of its heartbeat and joins the group again as a new member.
     */
    public function testAConsumerThatMissesItsSessionTimeoutRejoinsAsANewMember(): void
    {
        $groupId  = self::uniqueGroupName();
        $consumer = $this->consumer($groupId, [
            ConsumerConfig::SESSION_TIMEOUT_MS    => 1000,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 300,
        ]);
        $consumer->subscribe([$this->topic]);
        $this->pollUntil($consumer, 0, static fn(KafkaConsumer $polled): bool => $polled->assignment() !== []);

        $firstMemberId = array_key_first($this->describeGroup($groupId)->members);

        self::assertIsString($firstMemberId);

        // Nothing is polled for three session timeouts, so the coordinator drops this member
        sleep(3);

        $this->pollUntil($consumer, 0, static fn(KafkaConsumer $polled): bool => $polled->assignment() !== []);
        $description = $this->describeGroup($groupId);

        self::assertSame([0, 1, 2], $this->assignedPartitions($consumer), 'the consumer is consuming again');
        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $description->state);
        self::assertCount(1, $description->members);
        self::assertNotSame(
            $firstMemberId,
            array_key_first($description->members),
            'a member the coordinator dropped joins again without a member id and is given a new one'
        );
    }

    /**
     * Two consumers of one group split the partitions of the topic between themselves, and the one that stays
     * takes everything back as soon as the other one leaves.
     *
     * The member ids the coordinator hands out are `<client id>-<uuid>`, so the consumer of this process sorts
     * before the one of the child process (a hex digit against the `m` of `-member`), which makes the share of
     * each of them deterministic for both assignors.
     *
     * @param list<int> $expectedOwnPartitions    Partitions the consumer of this process has to receive
     * @param list<int> $expectedMemberPartitions Partitions the member in the child process has to receive
     */
    #[DataProvider('assignmentStrategies')]
    public function testTwoConsumersOfOneGroupSplitThePartitionsOfTheTopic(
        string $strategy,
        array $expectedOwnPartitions,
        array $expectedMemberPartitions
    ): void {
        $groupId = self::uniqueGroupName();
        $this->produce(0, ['p0']);
        $this->produce(1, ['p1']);
        $this->produce(2, ['p2']);

        $member = $this->startMember($groupId, $strategy);
        $this->awaitMemberStart($member);

        $consumer = $this->consumer($groupId, [ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => $strategy]);
        $consumer->subscribe([$this->topic]);

        $deadline = microtime(true) + self::REBALANCE_TIMEOUT;
        do {
            $consumer->poll(250);
            $member->readEvents();
            $ownPartitions    = $this->assignedPartitions($consumer);
            $memberPartitions = $member->getPartitionsOf($this->topic);
            $isSplit          = $ownPartitions === $expectedOwnPartitions
                && $memberPartitions === $expectedMemberPartitions;
        } while (!$isSplit && microtime(true) < $deadline);

        self::assertSame('', $member->getErrorOutput(), 'the member in the child process failed');
        self::assertSame($expectedOwnPartitions, $ownPartitions, 'share of the consumer of this process');
        self::assertSame($expectedMemberPartitions, $memberPartitions, 'share of the member in the child process');
        self::assertEqualsCanonicalizing(
            [0, 1, 2],
            array_merge($ownPartitions, $memberPartitions),
            'the two shares are disjoint and cover every partition of the topic'
        );
        self::assertCount(2, $this->describeGroup($groupId)->members, 'both members are in the group');

        // The second member leaves the group, which the consumer of this process notices on its next polls
        $member->requestStop();

        $deadline = microtime(true) + self::REBALANCE_TIMEOUT;
        do {
            $consumer->poll(250);
            $ownPartitions = $this->assignedPartitions($consumer);
        } while ($ownPartitions !== [0, 1, 2] && microtime(true) < $deadline);

        self::assertSame(
            [0, 1, 2],
            $ownPartitions,
            'the remaining consumer takes every partition over once the other member has left'
        );
    }

    /**
     * @return array<string, array{string, list<int>, list<int>}>
     */
    public static function assignmentStrategies(): array
    {
        return [
            // range: the partitions of the topic in order, the first member gets ceil(3/2) of them
            'range'      => [RangeAssignor::NAME, [0, 1], [2]],
            // roundrobin: the partitions handed out one by one, in the order of the member ids
            'roundrobin' => [RoundRobinAssignor::NAME, [0, 2], [1]],
        ];
    }

    /**
     * Polls the consumer until it received the expected number of records, or until the given condition holds
     *
     * @param callable(KafkaConsumer): bool|null $until Condition that ends the loop, records counting by default
     *
     * @return array<int, list<Record>> Records of every partition, in the order they arrived
     */
    private function pollUntil(KafkaConsumer $consumer, int $expectedRecords, ?callable $until = null): array
    {
        $received = [];
        $total    = 0;
        $deadline = microtime(true) + self::REBALANCE_TIMEOUT;

        do {
            foreach ($consumer->poll(250) as $partitions) {
                foreach ($partitions as $partition => $records) {
                    foreach ($records as $record) {
                        $received[$partition][] = $record;
                        $total++;
                    }
                }
            }
            $isDone = $until !== null ? $until($consumer) : $total >= $expectedRecords;
        } while (!$isDone && microtime(true) < $deadline);

        if ($until === null) {
            self::assertGreaterThanOrEqual(
                $expectedRecords,
                $total,
                sprintf('only %d of the %d expected records arrived', $total, $expectedRecords)
            );
        }

        return $received;
    }

    /**
     * Waits until the member in the child process has subscribed to the topic
     */
    private function awaitMemberStart(ConsumerGroupMemberProcess $member): void
    {
        $deadline = microtime(true) + self::REBALANCE_TIMEOUT;
        do {
            $member->readEvents();
            if ($member->hasStarted()) {
                return;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        self::fail('The second member of the group did not start: ' . $member->getErrorOutput());
    }

    /**
     * Starts a second member of the given group in a child process
     */
    private function startMember(string $groupId, string $strategy): ConsumerGroupMemberProcess
    {
        $member = new ConsumerGroupMemberProcess([
            'bootstrapServer'     => 'tcp://' . self::firstBootstrapServer(),
            'clientId'            => self::MEMBER_CLIENT_ID,
            'topic'               => $this->topic,
            'groupId'             => $groupId,
            'strategy'            => $strategy,
            'sessionTimeoutMs'    => self::SESSION_TIMEOUT_MS,
            'heartbeatIntervalMs' => 500,
            'requestTimeoutMs'    => self::REQUEST_TIMEOUT_MS,
            'durationSeconds'     => 2 * self::REBALANCE_TIMEOUT,
        ]);

        $this->members[] = $member;

        return $member;
    }

    /**
     * Returns the partitions of the topic under test that the consumer holds, in numeric order
     *
     * @return list<int>
     */
    private function assignedPartitions(KafkaConsumer $consumer): array
    {
        $partitions = array_map(intval(...), array_values($consumer->assignment()[$this->topic] ?? []));
        sort($partitions);

        return $partitions;
    }

    /**
     * Asks the coordinator what it knows about the group, with the api of `kafka-consumer-groups.sh --describe`
     */
    private function describeGroup(string $groupId): DescribeGroupResponseMetadata
    {
        $configuration = $this->configuration();

        return new AdminClient(Cluster::bootstrap($configuration), $configuration)->describeGroup($groupId);
    }

    /**
     * Builds a consumer for the broker under test and registers it for the clean-up
     *
     * @param array<string, mixed> $configuration Options that override the defaults of this test class
     */
    private function consumer(string $groupId, array $configuration = []): KafkaConsumer
    {
        $consumer = new KafkaConsumer(
            $configuration + [ConsumerConfig::GROUP_ID => $groupId] + $this->configuration()
        );

        $this->consumers[] = $consumer;

        return $consumer;
    }

    /**
     * @return array<string, mixed> Client configuration of this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,

            ConsumerConfig::SESSION_TIMEOUT_MS      => self::SESSION_TIMEOUT_MS,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS   => 500,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ];
    }

    /**
     * Produces the given values into one partition of the topic under test
     *
     * A partition whose leader has just been elected still answers LeaderNotAvailable (5) or NotLeaderForPartition
     * (6) for a moment, so the request is repeated while that is the case.
     *
     * @param list<string> $values Values of the records to append
     */
    private function produce(int $partition, array $values): void
    {
        $messageSet = MessageSet::fromRecords(array_map(
            static fn(string $value): Record => new Record($value),
            $values
        ));
        $deadline   = microtime(true) + self::TOPIC_TIMEOUT;

        do {
            $stream = $this->connect();
            new ProduceRequest(
                [$this->topic => [$partition => $messageSet]],
                1,
                self::PRODUCE_TIMEOUT_MS,
                self::CLIENT_ID,
                1
            )->writeTo($stream);

            $errorCode = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[$partition]->errorCode;
            if ($errorCode === KafkaException::NO_ERROR) {
                return;
            }
            if (!in_array($errorCode, self::NOT_SERVABLE_YET, true)) {
                throw KafkaException::fromCode($errorCode, ['topic' => $this->topic, 'partitionId' => $partition]);
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail(sprintf('The partition %s-%d never became servable', $this->topic, $partition));
    }

    /**
     * Returns the value of a consumed record
     */
    private static function valueOf(Record $record): ?string
    {
        return $record->value;
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't7-group-' . bin2hex(random_bytes(6));
    }
}
