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
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRebalanceListener;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribedGroup;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceResponseV2;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * A real consumer of this client with `group.protocol=consumer`, end to end against the 3.9.2 node.
 *
 * This is the wave's own question: does the KIP-848 path of {@see KafkaConsumer} really consume - join, receive a
 * server-side assignment, fetch, commit with the **member epoch** in the `generation_id_or_member_epoch` of an
 * OffsetCommit **v9**, read those offsets back with an OffsetFetch **v9** that names the member, and leave with
 * the heartbeat of the epoch -1 - and does the coordinator describe what it sent?
 *
 * The classic path of the same consumer is untouched and is covered by {@see ConsumerGroupTest}; the two are
 * compared here only where the difference is the point, i.e. at the incremental rebalance.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 * @see docs/protocol/3.9.md, section "The member epoch of KIP-848 (v9)"
 */
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(ConsumerGroupHeartbeatCoordinator::class)]
final class Kip848ConsumerTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-848-e2e';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const int MAX_POLL_INTERVAL_MS = 20000;

    private const int PRODUCE_TIMEOUT_MS = 5000;

    private const float TOPIC_TIMEOUT = 30.0;

    private const float POLL_TIMEOUT = 60.0;

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

    private string $topic;

    /**
     * @var list<KafkaConsumer>
     */
    private array $consumers = [];

    /**
     * @var list<string>
     */
    private static array $groups = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t3-848-e2e');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        foreach ($this->consumers as $consumer) {
            try {
                $consumer->unsubscribe();
            } catch (KafkaException) {
                // A member the coordinator has already forgotten must not fail the suite
            }
        }
        $this->consumers = [];

        parent::tearDown();
    }

    public static function tearDownAfterClass(): void
    {
        foreach (self::$groups as $groupId) {
            self::deleteGroupQuietly($groupId);
        }
        self::$groups = [];

        parent::tearDownAfterClass();
    }

    /**
     * The whole path: subscribe, poll, consume, commit with the member epoch, read the offsets back
     */
    public function testAConsumerOfTheNewProtocolConsumesAndCommitsWithItsMemberEpoch(): void
    {
        $groupId = $this->uniqueGroupName();
        $this->produce(0, ['p0-a', 'p0-b']);
        $this->produce(1, ['p1-a']);
        $this->produce(2, ['p2-a']);

        $consumer = $this->consumer($groupId);
        $consumer->subscribe([$this->topic]);

        self::assertSame([], $consumer->assignment(), 'the group is joined by the first poll(), not by subscribe()');

        $records = $this->pollUntil($consumer, 4);

        self::assertSame([0, 1, 2], $this->assignedPartitions($consumer), 'the only member gets every partition');
        self::assertSame(['p0-a', 'p0-b'], array_map(self::valueOf(...), $records[0] ?? []));
        self::assertSame(['p1-a'], array_map(self::valueOf(...), $records[1] ?? []));
        self::assertSame(['p2-a'], array_map(self::valueOf(...), $records[2] ?? []));

        $metadata = $consumer->groupMetadata();

        self::assertSame($groupId, $metadata->groupId);
        self::assertGreaterThan(0, $metadata->generationId, 'the member epoch stands where a generation stood');
        self::assertNotSame('', $metadata->memberId);

        // the positions are committed with that epoch, which only a v9 commit may carry
        $consumer->commitSync();

        self::assertSame(
            [$this->topic => [0 => 2, 1 => 1, 2 => 1]],
            $consumer->committed([$this->topic => [0, 1, 2]]),
            'and read back with the OffsetFetch v9 of the same member'
        );

        // and the coordinator describes exactly what this consumer sent it
        $description = $this->admin()->describeConsumerGroup($groupId);

        self::assertSame(ConsumerGroupDescribedGroup::STATE_STABLE, $description->state);
        self::assertCount(1, $description->members);
        self::assertSame([$this->topic => [0, 1, 2]], $description->assignment());

        $member = $description->members[$metadata->memberId] ?? null;

        self::assertNotNull($member, 'the member id the consumer generated for itself is the one the group holds');
        self::assertSame($metadata->generationId, $member->memberEpoch, 'and its epoch is the one it commits with');
        self::assertSame(self::CLIENT_ID, $member->clientId);
        self::assertSame([$this->topic], $member->subscribedTopicNames);
        self::assertTrue($member->isReconciled());
    }

    /**
     * The group of such a consumer is listed as a `consumer` group, never as a classic one
     */
    public function testTheGroupOfSuchAConsumerIsOfTheTypeConsumer(): void
    {
        $groupId = $this->uniqueGroupName();
        $this->produce(0, ['only-one']);

        $consumer = $this->consumer($groupId);
        $consumer->subscribe([$this->topic]);
        $this->pollUntil($consumer, 1);

        $listed = $this->admin()->listAllGroups();

        self::assertArrayHasKey($groupId, $listed);
        self::assertSame('consumer', $listed[$groupId]->groupType, 'the group type of KIP-848 (ListGroups v5)');
        self::assertSame('consumer', $listed[$groupId]->protocolType, 'which is NOT the protocol type');

        // and the classic describe has nothing to say about it, which is the routing rule of an admin client
        self::assertSame('Dead', $this->admin()->describeGroup($groupId)->state);
    }

    /**
     * A second member takes partitions over, and the listener of the first one sees only what it really lost
     */
    public function testTheRebalanceOfThisProtocolIsIncremental(): void
    {
        $groupId = $this->uniqueGroupName();
        $this->produce(0, ['p0']);
        $this->produce(1, ['p1']);
        $this->produce(2, ['p2']);

        $listener = new class implements ConsumerRebalanceListener {
            /** @var list<array<string, list<int>>> */
            public array $revoked = [];

            /** @var list<array<string, list<int>>> */
            public array $assigned = [];

            public function onPartitionsRevoked(array $partitions): void
            {
                $this->revoked[] = $partitions;
            }

            public function onPartitionsAssigned(array $partitions): void
            {
                $this->assigned[] = $partitions;
            }
        };

        $first = $this->consumer($groupId);
        $first->subscribe([$this->topic], $listener);
        $this->pollUntil($first, 3);

        self::assertSame([0, 1, 2], $this->assignedPartitions($first));
        self::assertSame([], $listener->revoked, 'nothing was revoked on the first join');
        self::assertSame([[$this->topic => [0, 1, 2]]], $listener->assigned);

        // A second member joins the same group. Both of them have to keep polling for the reconciliation to move
        // at all: the first one only learns what it loses in a heartbeat of its own, and the coordinator hands
        // the partitions to the second one only once the first has acknowledged the loss
        $second = $this->consumer($groupId, [ClientConfig::CLIENT_ID => self::CLIENT_ID . '-two']);
        $second->subscribe([$this->topic]);

        $deadline = microtime(true) + self::POLL_TIMEOUT;
        do {
            $second->poll(250);
            $first->poll(250);
            $settled = count($this->assignedPartitions($first)) < 3 && $second->assignment() !== [];
        } while (!$settled && microtime(true) < $deadline);

        self::assertTrue($settled, 'the group never settled on two members');

        $kept = $this->assignedPartitions($first);

        self::assertNotSame([0, 1, 2], $kept, 'the first member gave partitions up');
        self::assertCount(1, $listener->revoked, 'exactly one revocation');
        self::assertSame(
            array_values(array_diff([0, 1, 2], $kept)),
            $listener->revoked[0][$this->topic] ?? [],
            'and it named only the partitions that really moved, never the whole assignment'
        );
        self::assertSame(
            [],
            $listener->assigned[1][$this->topic] ?? [],
            'nothing was added to this member, so nothing is announced as assigned'
        );
    }

    /**
     * `group.protocol` accepts the two protocols of Kafka 3.9 and nothing else
     */
    public function testAnUnknownGroupProtocolIsRefusedByTheConsumer(): void
    {
        $consumer = $this->consumer($this->uniqueGroupName(), [ConsumerConfig::GROUP_PROTOCOL => 'share']);

        $this->expectException(InvalidConfigurationException::class);

        // The membership is built when the consumer first needs its coordinator, which subscribe() already does
        $consumer->subscribe([$this->topic]);
    }

    /**
     * Produces the given values into one partition of the topic of this test
     *
     * @param list<string> $values
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
            new ProduceRequestV2(
                [$this->topic => [$partition => $messageSet]],
                1,
                self::PRODUCE_TIMEOUT_MS,
                self::CLIENT_ID,
                1
            )->writeTo($stream);

            $errorCode = ProduceResponseV2::unpack($stream)->topics[$this->topic]->partitions[$partition]->errorCode;
            if ($errorCode === KafkaException::NO_ERROR) {
                return;
            }
            if (!in_array($errorCode, self::NOT_SERVABLE_YET, true)) {
                throw KafkaException::fromCode($errorCode, ['topic' => $this->topic, 'partitionId' => $partition]);
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The partition {$partition} of {$this->topic} never became servable");
    }

    /**
     * Polls until the expected number of records arrived, or until `$until` says the test may go on
     *
     * @param callable(KafkaConsumer): bool|null $until
     *
     * @return array<int, list<Record>>
     */
    private function pollUntil(KafkaConsumer $consumer, int $expectedRecords, ?callable $until = null): array
    {
        $received = [];
        $total    = 0;
        $deadline = microtime(true) + self::POLL_TIMEOUT;

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
        } else {
            self::assertTrue($isDone, 'the state this poll loop waited for never arrived');
        }

        return $received;
    }

    /**
     * @return list<int>
     */
    private function assignedPartitions(KafkaConsumer $consumer): array
    {
        $partitions = array_map(intval(...), array_values($consumer->assignment()[$this->topic] ?? []));
        sort($partitions);

        return $partitions;
    }

    /**
     * Builds a consumer of the NEW protocol and registers it for the clean-up
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

    private function admin(): AdminClient
    {
        $configuration = $this->configuration();

        return new AdminClient(Cluster::bootstrap($configuration, $this->topic), $configuration);
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,

            ConsumerConfig::GROUP_PROTOCOL          => ConsumerConfig::GROUP_PROTOCOL_CONSUMER,
            ConsumerConfig::MAX_POLL_INTERVAL_MS    => self::MAX_POLL_INTERVAL_MS,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    private function uniqueGroupName(): string
    {
        $groupId        = 't3-848-e2e-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        return $groupId;
    }

    private static function deleteGroupQuietly(string $groupId): void
    {
        try {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
                ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
                ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,
            ] + ConsumerConfig::getDefaultConfiguration();

            new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups([$groupId]);
        } catch (KafkaException) {
            // A group that is gone, or one the reaper has not emptied yet, must not fail the suite
        }
    }

    private static function valueOf(Record $record): ?string
    {
        return $record->value;
    }
}
