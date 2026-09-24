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
use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidRecordStateException;
use Protocol\Kafka\Common\Errors\InvalidShareSessionEpochException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\ShareSessionNotFoundException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Consumer\AcknowledgeType;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\Internals\ShareInFlightBatch;
use Protocol\Kafka\Consumer\Internals\ShareMembershipManager;
use Protocol\Kafka\Consumer\Internals\ShareSessionHandler;
use Protocol\Kafka\Consumer\KafkaShareConsumer;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\ShareGroupDescribedGroup;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;
use Protocol\Kafka\Protocol\Request\ShareFetchRequest;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * The share consumer of KIP-932 against the 4.3.1 KRaft node: {@see KafkaShareConsumer} over a share-group membership
 * (ShareGroupHeartbeat, key 76) and share sessions on the leader (ShareFetch and ShareAcknowledge, keys 78 and 79).
 *
 * Every test has a topic and a share group of its own; the group reads from `earliest` (the group config
 * `share.auto.offset.reset`, set through the config resource type 32 before anybody joins), and some tests lower the
 * group configs `share.record.lock.duration.ms` and `share.delivery.count.limit` to the lowest values the node
 * accepts. Every consumer is closed in {@see self::tearDown()} - which closes its share sessions and leaves the group
 * with the epoch -1 - and every group is deleted in {@see self::tearDownAfterClass()}.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
#[CoversClass(KafkaShareConsumer::class)]
#[CoversClass(ShareMembershipManager::class)]
#[CoversClass(ShareSessionHandler::class)]
#[CoversClass(ShareInFlightBatch::class)]
final class KafkaShareConsumerTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-s-sc';

    private const string TOPIC_PREFIX = 't3-s-sc';

    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * The group config resource of KIP-848 and KIP-932 (`ConfigResource.Type.GROUP`, id 32)
     */
    private const int GROUP_CONFIG_RESOURCE = 32;

    /**
     * How long a poll() waits for records that are expected, in milliseconds
     */
    private const int POLL_TIMEOUT_MS = 15000;

    /**
     * The lowest `share.record.lock.duration.ms` the node accepts (`group.share.min.record.lock.duration.ms`)
     */
    private const int MIN_LOCK_DURATION_MS = 15000;

    private static ?Cluster $sharedCluster = null;

    /**
     * Every group this class created
     *
     * @var list<string>
     */
    private static array $groups = [];

    /**
     * Consumers of the running test, closed in tearDown
     *
     * @var list<KafkaShareConsumer>
     */
    private array $consumers = [];

    protected function tearDown(): void
    {
        foreach ($this->consumers as $consumer) {
            try {
                $consumer->close();
            } catch (\Throwable) {
                // A consumer a test closed already, or one whose node is gone, must not fail the suite
            }
        }
        $this->consumers = [];

        parent::tearDown();
    }

    public static function tearDownAfterClass(): void
    {
        foreach (self::$groups as $groupId) {
            try {
                $configuration = self::baseConfiguration() + ConsumerConfig::getDefaultConfiguration();
                new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups([$groupId]);
            } catch (KafkaException) {
                // A group that is gone already must not fail the suite
            }
        }
        self::$groups = [];

        parent::tearDownAfterClass();
    }

    /**
     * The first poll() joins the group, is assigned the partition on a later heartbeat and returns the acquired records
     * with the delivery count 1 and the lock timeout of the group; the member is in the group under its own id
     */
    public function testTheFirstPollJoinsTheGroupAndReturnsTheAcquiredRecords(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $consumer          = $this->consumer($groupId);
        $consumer->subscribe([$topic]);
        self::assertSame([$topic], $consumer->subscription());
        self::assertNull($consumer->acquisitionLockTimeoutMs(), 'no answer named a lock timeout yet');

        $records = $this->pollUntil($consumer, 3);

        self::assertSame(['v0', 'v1', 'v2'], self::valuesOf($records));
        self::assertSame([1, 1, 1], array_map(static fn(ConsumerRecord $record): ?int => $record->deliveryCount, $records));
        self::assertSame([$topic], array_values(array_unique(array_map(static fn(ConsumerRecord $record): string => $record->topic, $records))));
        self::assertSame(30000, $consumer->acquisitionLockTimeoutMs(), 'share.record.lock.duration.ms of the group');

        $group = $this->admin()->describeShareGroup($groupId);
        self::assertSame(ShareGroupDescribedGroup::STATE_STABLE, $group->groupState);
        self::assertCount(1, $group->members);
        self::assertSame(22, strlen((string) array_key_first($group->members)), 'the member id the consumer generated');
    }

    /**
     * Implicit mode: the next poll() accepts what the last one returned, the acknowledgement rides on its ShareFetch,
     * and nobody is delivered the accepted records again; acknowledge() is refused
     */
    public function testTheImplicitModeAcceptsTheRecordsOfThePreviousPollOnTheNextOne(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $completed         = [];
        $consumer          = $this->consumer($groupId);
        $consumer->setAcknowledgementCommitCallback(static function (array $offsets, ?KafkaException $exception) use (&$completed): void {
            $completed[] = [$offsets, $exception];
        });
        $consumer->subscribe([$topic]);

        $records = $this->pollUntil($consumer, 3);
        try {
            $consumer->acknowledge($records[0]);
            self::fail('acknowledge() is refused in the implicit mode');
        } catch (\LogicException $refused) {
            self::assertSame('Implicit acknowledgement of delivery is being used.', $refused->getMessage());
        }

        self::assertSame([], $consumer->poll(1000), 'nothing new to deliver');
        self::assertSame([[[$topic => [0 => [0, 1, 2]]], null]], $completed, 'accepted with the next ShareFetch');
        $consumer->close();

        $second = $this->consumer($groupId);
        $second->subscribe([$topic]);
        self::assertSame([], $second->poll(3000), 'the accepted records are never delivered again');
    }

    /**
     * Explicit mode: a poll() refuses to run while a record of the last one has no acknowledgement, and a record that
     * is not in flight cannot be acknowledged
     */
    public function testTheExplicitModeRefusesAPollBeforeEveryRecordIsAcknowledged(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $consumer          = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $consumer->subscribe([$topic]);

        $records = $this->pollUntil($consumer, 3);
        $consumer->acknowledge($records[0]);
        try {
            $consumer->poll(100);
            self::fail('a poll() with unacknowledged records is refused');
        } catch (\LogicException $refused) {
            self::assertSame('All records must be acknowledged in explicit acknowledgement mode.', $refused->getMessage());
        }

        $consumer->acknowledge($records[1], AcknowledgeType::ACCEPT);
        $consumer->acknowledge($topic, 0, 2, AcknowledgeType::ACCEPT);
        self::assertSame([], $consumer->poll(1000));

        $this->expectException(\LogicException::class);
        $consumer->acknowledge($records[0], AcknowledgeType::ACCEPT);
    }

    /**
     * A released record comes back at once with the next delivery count, as many times as it is released
     */
    public function testReleasedRecordsComeBackWithTheNextDeliveryCount(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1']);
        $consumer          = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $consumer->subscribe([$topic]);

        $deliveries = [];
        $records    = $this->pollUntil($consumer, 2);
        for ($round = 0; $round < 3; $round++) {
            $deliveries[] = array_map(static fn(ConsumerRecord $record): array => [$record->offset, $record->deliveryCount], $records);
            foreach ($records as $record) {
                $consumer->acknowledge($record, $round < 2 ? AcknowledgeType::RELEASE : AcknowledgeType::ACCEPT);
            }
            if ($round < 2) {
                $records = $this->pollUntil($consumer, 2);
            }
        }

        self::assertSame([[[0, 1], [1, 1]], [[0, 2], [1, 2]], [[0, 3], [1, 3]]], $deliveries);
        self::assertSame([null], $consumer->commitSync()[$topic]);
    }

    /**
     * A rejected record is archived: never delivered again, to this member or another one
     */
    public function testARejectedRecordIsNeverDeliveredAgain(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $consumer          = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $consumer->subscribe([$topic]);

        [$first, $second, $third] = $this->pollUntil($consumer, 3);
        $consumer->acknowledge($first, AcknowledgeType::REJECT);
        $consumer->acknowledge($second, AcknowledgeType::RELEASE);
        $consumer->acknowledge($third, AcknowledgeType::RELEASE);

        $again = $this->pollUntil($consumer, 2);
        self::assertSame([[1, 2], [2, 2]], array_map(static fn(ConsumerRecord $record): array => [$record->offset, $record->deliveryCount], $again));
        foreach ($again as $record) {
            $consumer->acknowledge($record, AcknowledgeType::RELEASE);
        }
        $consumer->close();

        $other = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $other->subscribe([$topic]);
        $records = $this->pollUntil($other, 2);
        self::assertSame([[1, 3], [2, 3]], array_map(static fn(ConsumerRecord $record): array => [$record->offset, $record->deliveryCount], $records), 'the rejected 0 is not among them');
        foreach ($records as $record) {
            $other->acknowledge($record);
        }
        self::assertSame([], $other->poll(2000));
    }

    /**
     * `share.delivery.count.limit` archives a record once it was delivered that many times
     */
    public function testTheDeliveryCountLimitArchivesARecord(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0']);
        // 2 is the lowest limit the node accepts ("Value must be at least 2", the 40 for 1)
        $this->setGroupConfig($groupId, 'share.delivery.count.limit', '2');
        $consumer = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $consumer->subscribe([$topic]);

        $counts = [];
        foreach ([1, 2] as $ignored) {
            [$record] = $this->pollUntil($consumer, 1);
            $counts[] = $record->deliveryCount;
            $consumer->acknowledge($record, AcknowledgeType::RELEASE);
        }

        self::assertSame([1, 2], $counts);
        self::assertSame([], $consumer->poll(3000), 'the second release reached the limit: the record is archived');
    }

    /**
     * Two members of one group share the one partition: the records are spread between them, and no record is ever
     * held by both
     */
    public function testTwoMembersShareOnePartitionWithoutEverHoldingTheSameRecord(): void
    {
        [$topic, $groupId] = $this->shareGroup([]);
        $options           = [
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit',
            ConsumerConfig::SHARE_ACQUIRE_MODE         => ConsumerConfig::SHARE_ACQUIRE_MODE_RECORD_LIMIT,
            ConsumerConfig::MAX_POLL_RECORDS           => 2,
        ];
        $members = [$this->consumer($groupId, $options), $this->consumer($groupId, $options)];
        foreach ($members as $member) {
            $member->subscribe([$topic]);
            self::assertSame([], $member->poll(2000), 'the member joins an empty topic');
        }
        // Ten record batches of one record each, so that a batch never holds more than one poll() may take
        foreach (range(0, 9) as $i) {
            $this->produce($topic, ["v{$i}"]);
        }

        $seen     = [[], []];
        $deadline = microtime(true) + 60;
        while (count($seen[0]) + count($seen[1]) < 10 && microtime(true) < $deadline) {
            foreach ($members as $index => $member) {
                $records = $this->flatten($member->poll(1000));
                self::assertLessThanOrEqual(2, count($records), 'max.poll.records in the record-limit mode');
                foreach ($records as $record) {
                    self::assertSame(1, $record->deliveryCount, 'no record is delivered twice');
                    $seen[$index][] = (int) $record->offset;
                    $member->acknowledge($record);
                }
            }
        }

        self::assertSame([], array_intersect($seen[0], $seen[1]), 'no record went to both members');
        $all = array_merge($seen[0], $seen[1]);
        sort($all);
        self::assertSame(range(0, 9), $all);
        self::assertNotSame([], $seen[0], 'the first member got records');
        self::assertNotSame([], $seen[1], 'the second member got records');
        self::assertCount(2, $this->admin()->describeShareGroup($groupId)->members);
    }

    /**
     * The record-limit mode of KIP-1206: a poll() returns no more than `max.poll.records` records of a batch that
     * holds more
     */
    public function testTheRecordLimitModeReturnsNoMoreThanMaxPollRecords(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2', 'v3', 'v4', 'v5']);
        $consumer          = $this->consumer($groupId, [
            ConsumerConfig::SHARE_ACQUIRE_MODE => ConsumerConfig::SHARE_ACQUIRE_MODE_RECORD_LIMIT,
            ConsumerConfig::MAX_POLL_RECORDS   => 2,
        ]);
        $consumer->subscribe([$topic]);

        $sizes = [];
        for ($i = 0; $i < 3; $i++) {
            $sizes[] = count($this->flatten($consumer->poll(self::POLL_TIMEOUT_MS)));
        }

        self::assertSame([2, 2, 2], $sizes, 'one batch of six records, delivered two at a time');
    }

    /**
     * commitSync() sends the acknowledgements in a ShareAcknowledge and answers every partition; the callback sees the
     * offsets of each partition, commitAsync() reports through it alone, and the lock timeout of the ShareAcknowledge
     * v2 answer is the one acquisitionLockTimeoutMs() reports
     */
    public function testCommitSyncAndCommitAsyncReportEveryPartitionToTheCallback(): void
    {
        [$topic, $groupId] = $this->shareGroup([], 2);
        $this->produce($topic, ['a0', 'a1'], 0);
        $this->produce($topic, ['b0'], 1);
        $this->setGroupConfig($groupId, 'share.record.lock.duration.ms', (string) self::MIN_LOCK_DURATION_MS);
        $completed = [];
        $consumer  = $this->consumer($groupId, [
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit',
            // The node holds the first ShareFetch for its whole wait and answers both partitions at once
            ConsumerConfig::FETCH_MIN_BYTES            => 1048576,
        ]);
        $consumer->setAcknowledgementCommitCallback(static function (array $offsets, ?KafkaException $exception) use (&$completed): void {
            $completed[] = [$offsets, $exception];
        });
        $consumer->subscribe([$topic]);

        foreach ($this->pollUntil($consumer, 3) as $record) {
            $consumer->acknowledge($record);
        }
        self::assertSame(self::MIN_LOCK_DURATION_MS, $consumer->acquisitionLockTimeoutMs());

        $results = $consumer->commitSync();
        ksort($results[$topic]);
        self::assertSame([$topic => [0 => null, 1 => null]], $results);
        usort($completed, static fn(array $left, array $right): int => array_key_first($left[0][$topic]) <=> array_key_first($right[0][$topic]));
        self::assertSame([[[$topic => [0 => [0, 1]]], null], [[$topic => [1 => [0]]], null]], $completed);
        self::assertSame([], $consumer->commitSync(), 'nothing left to commit');

        $this->produce($topic, ['a2'], 0);
        $completed = [];
        foreach ($this->pollUntil($consumer, 1) as $record) {
            $consumer->acknowledge($record, AcknowledgeType::REJECT);
        }
        $consumer->commitAsync();
        self::assertSame([[[$topic => [0 => [2]]], null]], $completed);
    }

    /**
     * A renewal holds a record past its lock: after the lock duration the renewed record is still this member's, the
     * rest went to the other member with the next delivery count, and a late acknowledgement of those is the 121
     */
    public function testARenewHoldsARecordPastItsLock(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $this->setGroupConfig($groupId, 'share.record.lock.duration.ms', (string) self::MIN_LOCK_DURATION_MS);
        $completed = [];
        $holder    = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $holder->setAcknowledgementCommitCallback(static function (array $offsets, ?KafkaException $exception) use (&$completed): void {
            $completed[] = [$offsets, $exception === null ? null : $exception::class];
        });
        $other = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $holder->subscribe([$topic]);
        $other->subscribe([$topic]);

        [$renewed, $second, $third] = $this->pollUntil($holder, 3);
        $acquiredAt                 = microtime(true);
        self::assertSame([], $other->poll(1000), 'the other member joins; the records are locked');

        self::sleepUntil($acquiredAt + 10);
        $holder->acknowledge($renewed, AcknowledgeType::RENEW);
        self::assertSame([$topic => [0 => null]], $holder->commitSync());
        self::assertSame(self::MIN_LOCK_DURATION_MS, $holder->acquisitionLockTimeoutMs(), 'the ShareAcknowledge v2 answer');

        self::sleepUntil($acquiredAt + 18);
        $taken = $this->pollUntil($other, 2);
        self::assertSame([[1, 2], [2, 2]], array_map(static fn(ConsumerRecord $record): array => [$record->offset, $record->deliveryCount], $taken), 'the renewed 0 is not among them');

        $completed = [];
        $holder->acknowledge($second, AcknowledgeType::ACCEPT);
        $holder->acknowledge($third, AcknowledgeType::ACCEPT);
        $again = $holder->poll(1000);
        self::assertSame([[[$topic => [0 => [1, 2]]], InvalidRecordStateException::class]], $completed, 'the lock of 1 and 2 expired');
        self::assertSame([0], array_map(static fn(ConsumerRecord $record): ?int => $record->offset, $this->flatten($again)), 'the renewed record is returned again');

        $holder->acknowledge($this->flatten($again)[0]);
        self::assertSame([$topic => [0 => null]], $holder->commitSync(), 'the renewed record is still the holder\'s');
        foreach ($taken as $record) {
            $other->acknowledge($record);
        }
        self::assertSame([$topic => [0 => null]], $other->commitSync());
    }

    /**
     * close() sends the acknowledgements with the close of the share session, which releases the rest to another
     * member, and leaves the group; a closed consumer refuses every call
     */
    public function testCloseReleasesWhatIsHeldAndLeavesTheGroup(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $consumer          = $this->consumer($groupId, [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit']);
        $consumer->subscribe([$topic]);
        [$first] = $this->pollUntil($consumer, 3);
        $consumer->acknowledge($first);

        $consumer->close();

        self::assertSame(ShareGroupDescribedGroup::STATE_EMPTY, $this->admin()->describeShareGroup($groupId)->groupState, 'the member left with the epoch -1');
        $other = $this->consumer($groupId);
        $other->subscribe([$topic]);
        $records = $this->pollUntil($other, 2);
        self::assertSame([[1, 2], [2, 2]], array_map(static fn(ConsumerRecord $record): array => [$record->offset, $record->deliveryCount], $records), 'the accepted 0 went with the close, 1 and 2 were released');

        $this->expectException(\LogicException::class);
        $consumer->poll(100);
    }

    /**
     * unsubscribe() leaves the group; a new subscription joins it again under the same member id
     */
    public function testAMemberThatUnsubscribesRejoinsUnderItsMemberId(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0']);
        $consumer          = $this->consumer($groupId);
        $consumer->subscribe([$topic]);
        $this->pollUntil($consumer, 1);
        self::assertSame([$topic => [0 => null]], $consumer->commitSync(), 'the implicit acceptance of v0');
        $memberId = (string) array_key_first($this->admin()->describeShareGroup($groupId)->members);

        $consumer->unsubscribe();
        self::assertSame([], $consumer->subscription());
        self::assertSame(ShareGroupDescribedGroup::STATE_EMPTY, $this->admin()->describeShareGroup($groupId)->groupState);

        $this->produce($topic, ['v1']);
        $consumer->subscribe([$topic]);
        self::assertSame(['v1'], self::valuesOf($this->pollUntil($consumer, 1)));
        self::assertSame([$memberId], array_keys($this->admin()->describeShareGroup($groupId)->members));
    }

    /**
     * A session the node lost with its connection answers the next ShareFetch with the 122: the acknowledgements it
     * carried fail, the consumer opens a new session with the epoch 0, and the records come back
     */
    public function testASessionLostWithItsConnectionIsReopenedWithTheEpochZero(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $completed         = [];
        $consumer          = $this->consumer($groupId);
        $consumer->setAcknowledgementCommitCallback(static function (array $offsets, ?KafkaException $exception) use (&$completed): void {
            $completed[] = [$offsets, $exception === null ? null : $exception::class];
        });
        $consumer->subscribe([$topic]);
        $this->pollUntil($consumer, 3);

        // The connection drops: the node closes the share session that lived on it and releases what it held
        Node::closeConnections();

        $records = $this->pollUntil($consumer, 3);
        self::assertSame([[[$topic => [0 => [0, 1, 2]]], ShareSessionNotFoundException::class]], $completed, 'the acceptance rode on the lost session');
        self::assertSame([[0, 2], [1, 2], [2, 2]], array_map(static fn(ConsumerRecord $record): array => [$record->offset, $record->deliveryCount], $records));
    }

    /**
     * A session whose epoch went out of step answers the 123: the acknowledgements fail, and the consumer goes on in a
     * new session with the epoch 0
     */
    public function testASessionOutOfStepIsReopenedWithTheEpochZero(): void
    {
        [$topic, $groupId] = $this->shareGroup(['v0', 'v1', 'v2']);
        $completed         = [];
        $consumer          = $this->consumer($groupId);
        $consumer->setAcknowledgementCommitCallback(static function (array $offsets, ?KafkaException $exception) use (&$completed): void {
            $completed[] = [$offsets, $exception === null ? null : $exception::class];
        });
        $consumer->subscribe([$topic]);
        $this->pollUntil($consumer, 3);
        self::assertSame([], $consumer->poll(1000), 'the acceptance goes with the epoch 1, the wait moves the session on');
        self::assertSame([[[$topic => [0 => [0, 1, 2]]], null]], $completed);

        $this->produce($topic, ['v3', 'v4']);
        $this->pollUntil($consumer, 2);

        // Somebody else opens a session for this member on the same connection: the node replaces the session, whose
        // next epoch is 1 again, and the consumer's is far ahead of it
        $memberId = (string) array_key_first($this->admin()->describeShareGroup($groupId)->members);
        $leader   = $this->cluster()->leaderFor($topic, 0);
        new Client($this->cluster(), $this->configuration())->shareFetch($leader, $groupId, $memberId, ShareFetchRequest::INITIAL_EPOCH, [], [], 0);

        $completed = [];
        self::assertSame([], $consumer->poll(1000));
        self::assertSame([[[$topic => [0 => [3, 4]]], InvalidShareSessionEpochException::class]], $completed);

        $this->produce($topic, ['v5']);
        self::assertSame(['v5'], self::valuesOf($this->pollUntil($consumer, 1)), 'the new session fetches');
    }

    /**
     * Creates a topic with the given records and a share group that reads it from the earliest offset
     *
     * @param list<string> $values Records to produce into the partition 0 before anybody joins
     *
     * @return array{string, string} Topic name, group id
     */
    private function shareGroup(array $values, int $partitions = 1): array
    {
        $topic = self::uniqueTopicName(self::TOPIC_PREFIX);
        $this->admin()->createTopics([new NewTopic($topic, $partitions, 1)]);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::TOPIC_PREFIX)->awaitTopicWithLeaders($topic);
        if ($values !== []) {
            $this->produce($topic, $values);
        }

        $groupId        = 't3-s-sc-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;
        $this->setGroupConfig($groupId, 'share.auto.offset.reset', 'earliest');

        return [$topic, $groupId];
    }

    /**
     * Creates a share consumer of the group, closed in tearDown
     *
     * @param array<string, mixed> $options
     */
    private function consumer(string $groupId, array $options = []): KafkaShareConsumer
    {
        $consumer = new KafkaShareConsumer($options + [
            ConsumerConfig::GROUP_ID => $groupId,
        ] + self::baseConfiguration());
        $this->consumers[] = $consumer;

        return $consumer;
    }

    /**
     * Polls until the given number of records came back in one poll(), failing after the poll timeout
     *
     * @return list<ConsumerRecord>
     */
    private function pollUntil(KafkaShareConsumer $consumer, int $count): array
    {
        $records = $this->flatten($consumer->poll(self::POLL_TIMEOUT_MS));
        self::assertCount($count, $records, 'records of one poll()');

        return $records;
    }

    /**
     * @param array<string, array<int, list<ConsumerRecord>>> $records
     *
     * @return list<ConsumerRecord>
     */
    private function flatten(array $records): array
    {
        $flat = [];
        foreach ($records as $partitions) {
            foreach ($partitions as $partitionRecords) {
                array_push($flat, ...$partitionRecords);
            }
        }

        return $flat;
    }

    /**
     * @param list<ConsumerRecord> $records
     *
     * @return list<string|null>
     */
    private static function valuesOf(array $records): array
    {
        return array_map(static fn(ConsumerRecord $record): ?string => $record->value, $records);
    }

    /**
     * @param list<string> $values
     */
    private function produce(string $topic, array $values, int $partition = 0): void
    {
        $records = [];
        foreach ($values as $value) {
            $records[] = new Record($value, null, 0, null, (int) (microtime(true) * 1000));
        }

        $stream = $this->connect();
        new ProduceRequestV12([$topic => [$partition => RecordBatch::fromRecords($records)]], 1, 10000, self::CLIENT_ID)
            ->writeTo($stream);

        $errorCode = ProduceResponseV12::unpack($stream)->topics[$topic]->partitions[$partition]->errorCode;
        if ($errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($errorCode, ['topic' => $topic]);
        }
    }

    /**
     * Sets a group config through IncrementalAlterConfigs and the config resource type 32
     */
    private function setGroupConfig(string $groupId, string $name, string $value): void
    {
        $stream = $this->connect();
        new IncrementalAlterConfigsRequest(
            [new IncrementalAlterConfigsRequestResource(self::GROUP_CONFIG_RESOURCE, $groupId, [AlterConfigOp::set($name, $value)])],
            false,
            self::CLIENT_ID
        )->writeTo($stream);

        $answer = IncrementalAlterConfigsResponse::unpack($stream);
        self::assertSame(KafkaException::NO_ERROR, $answer->responses[0]->errorCode ?? null, "{$name} of {$groupId}");
    }

    private static function sleepUntil(float $moment): void
    {
        $left = $moment - microtime(true);
        if ($left > 0) {
            usleep((int) ($left * 1e6));
        }
    }

    private function admin(): AdminClient
    {
        return new AdminClient($this->cluster(), $this->configuration());
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return self::baseConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * @return array<string, mixed>
     */
    private static function baseConfiguration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,
        ];
    }
}
