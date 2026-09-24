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
use Protocol\Kafka\Admin\ListShareGroupOffsetsSpec;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\SharePartitionOffsetInfo;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\GroupNotEmptyException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;
use Protocol\Kafka\Protocol\Request\ShareFetchRequest;

/**
 * The share-group admin methods of {@see AdminClient} against the 4.3.1 node, which finalizes `share.version` 1:
 * `listShareGroups()`, `listShareGroupOffsets()`, `alterShareGroupOffsets()`, `deleteShareGroupOffsets()` and
 * `deleteShareGroups()`, each of them against every kind of group - one that does not exist, a classic group, a
 * KIP-848 group, a share group without a member and a share group with one.
 *
 * The classic group is created by an administrative commit, the KIP-848 group by a member that joins and leaves
 * again, the empty share group by an AlterShareGroupOffsets and the share group with a member by
 * {@see Client::joinShareGroup()}; that member leaves with the epoch -1 before its group is deleted. Every topic and
 * every group this class creates is deleted again.
 *
 * @see docs/protocol/4.3.md, section "The share-group admin methods"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ListShareGroupOffsetsSpec::class)]
#[CoversClass(SharePartitionOffsetInfo::class)]
final class ShareGroupAdminTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t4-s-admin';

    private const string PREFIX = 't4-s-admin';

    private const float TIMEOUT = 30.0;

    private const int GROUP_NOT_EMPTY = 68;

    private const int GROUP_ID_NOT_FOUND = 69;

    /**
     * The group config resource of KIP-848 and KIP-932 (`ConfigResource.Type.GROUP`, id 32)
     */
    private const int GROUP_CONFIG_RESOURCE = 32;

    private Cluster $cluster;

    private AdminClient $admin;

    private Client $client;

    /**
     * @var list<string>
     */
    private array $createdTopics = [];

    /**
     * @var list<string>
     */
    private array $createdGroups = [];

    /**
     * Share members that still have to leave, as [group, member id]
     *
     * @var list<array{0: string, 1: string}>
     */
    private array $shareMembers = [];

    private int $correlationId = 4300;

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
        $this->client  = new Client($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        foreach ($this->shareMembers as [$group, $member]) {
            $this->leaveShareGroup($group, $member);
        }
        $this->shareMembers = [];

        if ($this->createdGroups !== []) {
            try {
                $this->admin->deleteShareGroups($this->createdGroups);
            } catch (KafkaException) {
                // a coordinator that could not be looked up: the groups are left to the retention of the node
            }
            $this->createdGroups = [];
        }

        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testListShareGroupsListsTheShareGroupsAloneWithTheirState(): void
    {
        $topic   = $this->topic('list', 1);
        $classic = $this->classicGroup('list-classic', $topic);
        $kip848  = $this->kip848Group('list-kip848', $topic);
        $empty   = $this->emptyShareGroup('list-empty', $topic);
        $member  = $this->shareGroupWithMember('list-member', $topic);
        $ours    = array_flip([$classic, $kip848, $empty, $member]);

        $listed = $this->pollUntil(
            fn(): array => array_intersect_key($this->admin->listShareGroups(), $ours),
            static fn(array $groups): bool => count($groups) === 2
        );

        $expected = [$empty, $member];
        sort($expected);
        $names = array_map(strval(...), array_keys($listed));
        sort($names);
        self::assertSame($expected, $names, 'the two share groups, and neither the classic nor the KIP-848 group');
        self::assertSame(
            ['share', 'share', 'Empty'],
            [$listed[$empty]->protocolType, $listed[$empty]->groupType, $listed[$empty]->groupState]
        );
        self::assertSame(
            ['share', ListGroupResponseProtocol::TYPE_SHARE, 'Stable'],
            [$listed[$member]->protocolType, $listed[$member]->groupType, $listed[$member]->groupState]
        );

        self::assertSame(
            [$member],
            array_keys(array_intersect_key($this->admin->listShareGroups(['Stable']), $ours)),
            'the state filter of KIP-518 applies as well'
        );
        self::assertSame(
            [],
            array_intersect_key($this->admin->listConsumerGroups([], [ListGroupResponseProtocol::TYPE_SHARE]), $ours),
            'listConsumerGroups() keeps the protocol type `consumer` and never lists a share group'
        );
        $all = array_intersect_key($this->admin->listAllGroups(), $ours);
        self::assertSame(
            [ListGroupResponseProtocol::TYPE_CLASSIC, ListGroupResponseProtocol::TYPE_CONSUMER],
            [$all[$classic]->groupType, $all[$kip848]->groupType],
            'the other two groups exist, they are just not share groups'
        );
    }

    public function testListShareGroupOffsetsOfEveryKindOfGroup(): void
    {
        $topic   = $this->topic('list-offsets', 2);
        $missing = self::uniqueTopicName(self::PREFIX . '-missing');
        $this->produce($topic, 10);
        $unknown = $this->group('offsets-unknown');
        $classic = $this->classicGroup('offsets-classic', $topic);
        $kip848  = $this->kip848Group('offsets-kip848', $topic);
        $member  = $this->shareGroupWithMember('offsets-member', $topic);
        $share   = $this->group('offsets-share');
        self::assertSame(
            [$topic => [0 => null, 1 => null]],
            $this->admin->alterShareGroupOffsets($share, [$topic => [0 => 3, 1 => 0]])
        );

        $offsets = $this->pollUntil(
            fn(): array => $this->admin->listShareGroupOffsets([$share => null]),
            static fn(array $offsets): bool => ($offsets[$share][$topic][0] ?? null)?->lag === 7
        );
        self::assertEquals(
            [$share => [$topic => [0 => new SharePartitionOffsetInfo(3, 0, 7), 1 => new SharePartitionOffsetInfo(0, 0, 0)]]],
            self::sorted($offsets),
            'every partition the group holds state for: 10 - 3 records to deliver of the first, none of the empty second'
        );
        self::assertSame(
            ["{$share} {$topic} 0 3 7", "{$share} {$topic} 1 0 0"],
            $this->shareGroupsToolOffsets($share),
            '`kafka-share-groups.sh --describe --offsets` reads the same start offsets and lags'
        );

        $named = $this->admin->listShareGroupOffsets([
            $share   => new ListShareGroupOffsetsSpec([new TopicPartition($topic, 0), new TopicPartition($missing, 0)]),
            $unknown => [$topic => [0, 1]],
            $classic => [$topic => [0]],
            $kip848  => ListShareGroupOffsetsSpec::allPartitions(),
            $member  => null,
        ]);
        self::assertEquals(new SharePartitionOffsetInfo(3, 0, 7), $named[$share][$topic][0]);
        self::assertSame([0 => null], $named[$share][$missing], 'a topic the node does not have is no error');
        self::assertSame([$topic => [0 => null, 1 => null]], self::sorted($named[$unknown]), 'no 69 for a group that does not exist');
        self::assertSame([$topic => [0 => null]], $named[$classic], 'a classic group holds no share state');
        self::assertSame([], $named[$kip848], 'nor does a KIP-848 group');
        self::assertSame(
            [$topic => [0 => null, 1 => null]],
            self::sorted($this->pollUntil(
                fn(): array => $this->admin->listShareGroupOffsets([$member => null])[$member],
                static fn(array $offsets): bool => count($offsets[$topic] ?? []) === 2
            )),
            'a member that acknowledged nothing yet: the partitions of its subscription, without a start offset'
        );

        self::assertSame(
            [$unknown => [], $classic => []],
            $this->admin->listShareGroupOffsets([$unknown => null, $classic => null])
        );
        self::assertSame([], $this->admin->listShareGroupOffsets([]), 'nothing asked, nothing sent');
    }

    /**
     * The lag of KIP-1226 through the admin method, and what `kafka-share-groups.sh --describe --offsets` says
     */
    public function testListShareGroupOffsetsReportsTheLagOfAShareGroupWithAMember(): void
    {
        $topic   = $this->topic('lag', 1);
        $group   = $this->group('lag');
        $topicId = self::topicIdOf($topic);
        $this->produce($topic, 10);
        $this->setGroupConfig($group, 'share.auto.offset.reset', 'earliest');
        $this->cluster->reload([$topic]);
        $leader = $this->cluster->leaderFor($topic, 0);
        $member = $this->joinedShareMember($group, $topic, $topicId);
        $epoch  = ShareFetchRequest::INITIAL_EPOCH;

        try {
            $deadline = microtime(true) + self::TIMEOUT;
            do {
                $fetched  = $this->client->shareFetch($leader, $group, $member, $epoch++, [$topicId => [0]], [], 500);
                $acquired = $fetched->partitionOf($topicId, 0)?->acquiredRecords ?? [];
            } while ($acquired === [] && microtime(true) < $deadline);
            self::assertNotSame([], $acquired, 'the ten records are acquired');

            self::assertSame(
                [$group => [$topic => [0 => null]]],
                $this->admin->listShareGroupOffsets([$group => null]),
                'acquired but not acknowledged: no start offset yet'
            );

            $this->client->shareAcknowledge($leader, $group, $member, $epoch++, [$topicId => [0 => [
                ShareAcknowledgementBatch::of(0, 3, ShareAcknowledgementBatch::ACCEPT),
                ShareAcknowledgementBatch::of(4, 5, ShareAcknowledgementBatch::RELEASE),
                ShareAcknowledgementBatch::of(6, 9, ShareAcknowledgementBatch::ACCEPT),
            ]]]);
            // The start offset the share coordinator keeps may stay at 0 for a while; the lag is 2 either way
            $info = $this->pollUntil(
                fn(): ?SharePartitionOffsetInfo => $this->admin->listShareGroupOffsets([$group => null])[$group][$topic][0] ?? null,
                static fn(?SharePartitionOffsetInfo $info): bool => $info?->lag === 2
            );
            self::assertSame([0, 2], [$info?->leaderEpoch, $info?->lag], '10 records, 8 of them accepted: the released 4 and 5');

            $rows = $this->pollUntil(
                function () use ($group, $topic): array {
                    $info = $this->admin->listShareGroupOffsets([$group => null])[$group][$topic][0] ?? null;

                    return [$this->shareGroupsToolOffsets($group), ["{$group} {$topic} 0 {$info?->startOffset} {$info?->lag}"]];
                },
                static fn(array $rows): bool => $rows[0] === $rows[1]
            );
            self::assertSame($rows[1], $rows[0], 'the tool of the image reads the same start offset and lag');

            self::assertSame(
                [$topic => [0 => self::GROUP_NOT_EMPTY]],
                self::codesOf($this->admin->alterShareGroupOffsets($group, [$topic => [0 => 0]])),
                'a share group with a member keeps its start offsets'
            );
        } finally {
            $this->client->shareAcknowledge($leader, $group, $member, ShareFetchRequest::FINAL_EPOCH);
            $this->leaveShareGroup($group, $member);
            $this->shareMembers = [];
        }
    }

    public function testAlterShareGroupOffsetsAgainstEveryKindOfGroup(): void
    {
        $topic   = $this->topic('alter', 2);
        $missing = self::uniqueTopicName(self::PREFIX . '-missing');
        $created = $this->group('alter-created');
        $classic = $this->classicGroup('alter-classic', $topic);
        $kip848  = $this->kip848Group('alter-kip848', $topic);
        $member  = $this->shareGroupWithMember('alter-member', $topic);

        $result = $this->admin->alterShareGroupOffsets($created, [$topic => [0 => 5, 1 => 0, 2 => 0], $missing => [0 => 0]]);
        self::assertSame(
            [$topic => [0 => KafkaException::NO_ERROR, 1 => KafkaException::NO_ERROR, 2 => KafkaException::UNKNOWN_TOPIC_OR_PARTITION], $missing => [0 => KafkaException::UNKNOWN_TOPIC_OR_PARTITION]],
            self::codesOf($result),
            'the group is created, the partitions it can have are set, the others are the 3'
        );
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$missing][0]);
        self::assertSame('This server does not host this topic-partition.', $result[$missing][0]->getContext()['error'] ?? null);
        self::assertSame(
            'Empty',
            $this->pollUntil(
                fn(): ?string => $this->admin->listShareGroups()[$created]->groupState ?? null,
                static fn(?string $state): bool => $state !== null
            ),
            'the alter creates an empty share group'
        );

        foreach ([$classic, $kip848] as $other) {
            $refused = $this->admin->alterShareGroupOffsets($other, [$topic => [0 => 0, 1 => 0], $missing => [0 => 0]]);
            self::assertSame(
                [$topic => [0 => self::GROUP_ID_NOT_FOUND, 1 => self::GROUP_ID_NOT_FOUND], $missing => [0 => self::GROUP_ID_NOT_FOUND]],
                self::codesOf($refused),
                'the refusal of the whole group is reported for every partition'
            );
            self::assertInstanceOf(GroupIdNotFoundException::class, $refused[$topic][0]);
            self::assertSame("Group {$other} is not a share group.", $refused[$topic][0]->getContext()['error'] ?? null);
        }

        $nonEmpty = $this->admin->alterShareGroupOffsets($member, [$topic => [0 => 0]]);
        self::assertInstanceOf(GroupNotEmptyException::class, $nonEmpty[$topic][0]);
        self::assertSame('The group is not empty.', $nonEmpty[$topic][0]->getContext()['error'] ?? null);

        self::assertSame([], $this->admin->alterShareGroupOffsets($created, []), 'nothing to set, nothing sent');
    }

    public function testDeleteShareGroupOffsetsAgainstEveryKindOfGroup(): void
    {
        $topic   = $this->topic('delete-offsets', 1);
        $other   = $this->topic('delete-offsets-other', 1);
        $missing = self::uniqueTopicName(self::PREFIX . '-missing');
        $unknown = $this->group('delete-offsets-unknown');
        $classic = $this->classicGroup('delete-offsets-classic', $topic);
        $kip848  = $this->kip848Group('delete-offsets-kip848', $topic);
        $member  = $this->shareGroupWithMember('delete-offsets-member', $topic);
        $share   = $this->group('delete-offsets-share');
        $this->admin->alterShareGroupOffsets($share, [$topic => [0 => 0], $other => [0 => 0]]);
        $this->pollUntil(
            fn(): array => $this->admin->listShareGroupOffsets([$share => null])[$share],
            static fn(array $offsets): bool => count($offsets) === 2
        );

        $deleted = $this->admin->deleteShareGroupOffsets($share, [$topic, $missing, $topic]);
        self::assertSame([$topic, $missing], array_keys($deleted), 'one entry per topic, in the order of the call');
        self::assertNull($deleted[$topic]);
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $deleted[$missing]);
        self::assertSame(
            [$other],
            array_keys($this->pollUntil(
                fn(): array => $this->admin->listShareGroupOffsets([$share => null])[$share],
                static fn(array $offsets): bool => !isset($offsets[$topic])
            )),
            'the group forgot the one topic and keeps the other'
        );

        $refusals = [
            [$unknown, GroupIdNotFoundException::class, "Group {$unknown} not found."],
            [$classic, GroupIdNotFoundException::class, "Group {$classic} is not a share group."],
            [$kip848, GroupIdNotFoundException::class, "Group {$kip848} is not a share group."],
            [$member, GroupNotEmptyException::class, 'The group is not empty.'],
        ];
        foreach ($refusals as [$group, $exceptionClass, $message]) {
            try {
                $this->admin->deleteShareGroupOffsets($group, [$topic]);
                self::fail("The delete of the offsets of {$group} was not refused");
            } catch (KafkaException $exception) {
                self::assertInstanceOf($exceptionClass, $exception);
                self::assertSame($message, $exception->getContext()['error'] ?? null);
            }
        }
        self::assertSame(
            [],
            array_intersect_key($this->admin->listShareGroups(), [$unknown => true]),
            'unlike an alter, a delete creates no group'
        );

        self::assertSame([], $this->admin->deleteShareGroupOffsets($share, []), 'nothing to delete, nothing sent');
    }

    public function testDeleteShareGroupsAgainstEveryKindOfGroup(): void
    {
        $topic   = $this->topic('delete', 1);
        $unknown = $this->group('delete-unknown');
        $classic = $this->classicGroup('delete-classic', $topic);
        $kip848  = $this->kip848Group('delete-kip848', $topic);
        $empty   = $this->emptyShareGroup('delete-empty', $topic);
        $member  = $this->shareGroupWithMember('delete-member', $topic);

        $result = $this->admin->deleteShareGroups([$unknown, $member, $empty]);
        self::assertSame([$unknown, $member, $empty], array_keys($result), 'every group, in the order of the call');
        self::assertInstanceOf(GroupIdNotFoundException::class, $result[$unknown]);
        self::assertInstanceOf(GroupNotEmptyException::class, $result[$member], 'a share group with a member stays');
        self::assertNull($result[$empty]);

        self::assertSame(
            [$member],
            array_keys(array_intersect_key($this->admin->listShareGroups(), [$empty => true, $member => true]))
        );
        self::assertSame(
            [$empty => []],
            $this->admin->listShareGroupOffsets([$empty => null]),
            'the state of the deleted group is gone with it'
        );

        // Once its member left, the group is deleted as well
        $this->leaveShareGroup($member, $this->shareMembers[0][1]);
        $this->shareMembers = [];
        self::assertSame([$member => null], $this->admin->deleteShareGroups([$member]));

        // DeleteGroups does not know which type of group its caller meant: a classic and a KIP-848 group go as well
        self::assertSame([$classic => null, $kip848 => null], $this->admin->deleteShareGroups([$classic, $kip848]));
        self::assertSame([], array_intersect_key($this->admin->listAllGroups(), [$classic => true, $kip848 => true]));
    }

    /**
     * Error codes of an alter result, 0 for a partition that was set
     *
     * @param array<string, array<int, KafkaException|null>> $result
     *
     * @return array<string, array<int, int>>
     */
    private static function codesOf(array $result): array
    {
        return array_map(
            static fn(array $partitions): array => array_map(
                static fn(?KafkaException $error): int => $error === null ? KafkaException::NO_ERROR : $error->getCode(),
                $partitions
            ),
            $result
        );
    }

    /**
     * Sorts a result by topic and partition, which the node answers in the order of its own hash maps
     *
     * @template T of array
     *
     * @param T $result
     *
     * @return T
     */
    private static function sorted(array $result): array
    {
        ksort($result);
        foreach ($result as $key => $value) {
            if (is_array($value)) {
                $result[$key] = self::sorted($value);
            }
        }

        return $result;
    }

    /**
     * Calls the reader until its answer satisfies the predicate, and returns the last answer
     *
     * @template T
     *
     * @param callable(): T    $read
     * @param callable(T): bool $isFinal
     *
     * @return T
     */
    private function pollUntil(callable $read, callable $isFinal): mixed
    {
        $deadline = microtime(true) + self::TIMEOUT;
        do {
            $answer = $read();
            if ($isFinal($answer)) {
                return $answer;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        return $answer;
    }

    /**
     * A classic group without a member: the one an administrative commit creates
     */
    private function classicGroup(string $purpose, string $topic): string
    {
        $group = $this->group($purpose);
        $this->client->commitGroupOffsets($this->client->getGroupCoordinator($group), $group, '', -1, [$topic => [0 => 1]], -1);

        return $group;
    }

    /**
     * A KIP-848 group without a member: one member joins it and leaves again
     */
    private function kip848Group(string $purpose, string $topic): string
    {
        $group       = $this->group($purpose);
        $coordinator = new CoordinatorLookup($this->cluster, $this->configuration())->findCoordinator($group);
        $member      = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $this->client->joinConsumerGroup($coordinator, $group, $member, [$topic], 30000);
        $this->client->leaveConsumerGroup($coordinator, $group, $member);

        return $group;
    }

    /**
     * A share group without a member: the one an AlterShareGroupOffsets creates
     */
    private function emptyShareGroup(string $purpose, string $topic): string
    {
        $group = $this->group($purpose);
        self::assertSame([$topic => [0 => null]], $this->admin->alterShareGroupOffsets($group, [$topic => [0 => 0]]));

        return $group;
    }

    /**
     * A share group with one member, which leaves in {@see self::tearDown()} at the latest
     */
    private function shareGroupWithMember(string $purpose, string $topic): string
    {
        $group  = $this->group($purpose);
        $member = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $answer = $this->client->joinShareGroup(
            new CoordinatorLookup($this->cluster, $this->configuration())->findCoordinator($group),
            $group,
            $member,
            [$topic]
        );
        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        $this->shareMembers[] = [$group, $member];

        return $group;
    }

    /**
     * Joins a share member and heartbeats until the one partition of the topic is assigned to it
     */
    private function joinedShareMember(string $group, string $topic, string $topicId): string
    {
        $member      = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $coordinator = new CoordinatorLookup($this->cluster, $this->configuration())->findCoordinator($group);
        $answer      = $this->client->joinShareGroup($coordinator, $group, $member, [$topic]);
        $this->shareMembers[] = [$group, $member];

        $deadline = microtime(true) + self::TIMEOUT;
        while (($answer->assignment?->partitionsByTopicId()[$topicId] ?? []) === [] && microtime(true) < $deadline) {
            usleep(250000);
            $answer = $this->client->shareGroupHeartbeat($coordinator, $group, $member, $answer->memberEpoch);
        }
        self::assertSame([0], $answer->assignment?->partitionsByTopicId()[$topicId] ?? [], 'the partition is assigned');

        return $member;
    }

    private function leaveShareGroup(string $group, string $member): void
    {
        try {
            $this->client->leaveShareGroup(
                new CoordinatorLookup($this->cluster, $this->configuration())->findCoordinator($group),
                $group,
                $member
            );
        } catch (KafkaException) {
            // the member left already
        }
    }

    /**
     * The rows of `kafka-share-groups.sh --describe --offsets` for the group, with their columns single-spaced
     *
     * @return list<string>
     */
    private function shareGroupsToolOffsets(string $group): array
    {
        $output = (string) shell_exec(sprintf(
            'docker exec %s /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server localhost:9092'
            . ' --describe --offsets --group %s 2>&1',
            escapeshellarg(self::container()),
            escapeshellarg($group)
        ));

        $rows = [];
        foreach (explode("\n", $output) as $line) {
            if (str_starts_with($line, $group)) {
                $rows[] = (string) preg_replace('/\s+/', ' ', trim($line));
            }
        }

        return $rows;
    }

    /**
     * Appends the given number of records to the partition 0 of the topic, through the Produce v12 of a topic name
     */
    private function produce(string $topic, int $count): void
    {
        $records = [];
        for ($i = 0; $i < $count; $i++) {
            $records[] = new Record("v{$i}", null, 0, null, (int) (microtime(true) * 1000));
        }

        $stream = $this->connect();
        new ProduceRequestV12([$topic => [0 => RecordBatch::fromRecords($records)]], -1, 10000, self::CLIENT_ID, $this->correlationId++)
            ->writeTo($stream);
        self::assertSame(KafkaException::NO_ERROR, ProduceResponseV12::unpack($stream)->topics[$topic]->partitions[0]->errorCode);
    }

    /**
     * Sets a group config (KIP-848 and KIP-932) through IncrementalAlterConfigs and the config resource type 32
     */
    private function setGroupConfig(string $group, string $name, string $value): void
    {
        $stream = $this->connect();
        new IncrementalAlterConfigsRequest(
            [new IncrementalAlterConfigsRequestResource(self::GROUP_CONFIG_RESOURCE, $group, [AlterConfigOp::set($name, $value)])],
            false,
            self::CLIENT_ID,
            $this->correlationId++
        )->writeTo($stream);
        $answer = IncrementalAlterConfigsResponse::unpack($stream);
        self::assertSame(KafkaException::NO_ERROR, $answer->responses[0]->errorCode ?? null, "{$name} of {$group}");
    }

    private function topic(string $purpose, int $partitions): string
    {
        $topic                 = self::uniqueTopicName(self::PREFIX . "-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, $partitions, 1)]));

        $deadline = microtime(true) + self::TIMEOUT;
        do {
            try {
                $this->cluster->reload();
                $this->admin->listOffsets([$topic => range(0, $partitions - 1)], OffsetsRequest::LATEST);

                return $topic;
            } catch (KafkaException | TopicPartitionRequestException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    private function group(string $purpose): string
    {
        $group                 = self::PREFIX . "-{$purpose}-" . bin2hex(random_bytes(6));
        $this->createdGroups[] = $group;

        return $group;
    }

    /**
     * Name of the container the node of this line runs in
     */
    private static function container(): string
    {
        $container = getenv('KAFKA_CONTAINER');

        return $container === false || trim($container) === '' ? 'kafka-4-3-1' : trim($container);
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ] + ConsumerConfig::getDefaultConfiguration();
    }
}
