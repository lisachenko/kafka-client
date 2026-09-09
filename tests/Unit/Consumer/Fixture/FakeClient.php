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

namespace Protocol\Kafka\Tests\Unit\Consumer\Fixture;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\OffsetAndTimestamp;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMember;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Tests\Fixture\SpecMessageSet;

/**
 * An in-memory stand-in for the low-level client, so that the consumer can be driven without a broker.
 *
 * Every method of {@see Client} that the consumer uses is overridden here; the constructor deliberately does not
 * call the one of the parent, because neither the cluster nor the configuration of the real client is touched by
 * any of the overrides.
 */
final class FakeClient extends Client
{
    /**
     * Committed offsets of the groups, as [group][topic][partition] => offset
     *
     * @var array<string, array<string, array<int, int>>>
     */
    public array $committedOffsets = [];

    /**
     * Log of the commits that were made, in order, as ['group' => string, 'offsets' => array]
     *
     * @var list<array{group: string, offsets: array<string, array<int, mixed>>}>
     */
    public array $commits = [];

    /**
     * Positions that every fetch was called with, in order
     *
     * @var list<array<string, array<int, int>>>
     */
    public array $fetchCalls = [];

    /**
     * Requests that fetchTopicPartitionOffsets() was called with, in order
     *
     * @var list<array<string, array<int, int>>>
     */
    public array $offsetsCalls = [];

    /**
     * Log of one partition, as [topic][partition] => list of records with their offsets
     *
     * @var array<string, array<int, list<Record>>>
     */
    public array $log = [];

    /**
     * First offset that is still available in the log, as [topic][partition] => offset
     *
     * @var array<string, array<int, int>>
     */
    public array $logStartOffsets = [];

    /**
     * Log end offsets that override the ones derived from self::$log, as [topic][partition] => offset
     *
     * @var array<string, array<int, int>>
     */
    public array $logEndOffsets = [];

    /**
     * Exceptions to throw on the next fetch calls, one per call, in order
     *
     * @var list<\Throwable|null>
     */
    public array $fetchFailures = [];

    /**
     * Answer a fetch with the whole log of the partition, whatever offset was asked for.
     *
     * This is what a broker does for a compressed message set: the set is stored as one message and handed back as
     * a whole, so a fetch that starts in the middle of it also receives the records before the requested offset.
     */
    public bool $ignoreFetchOffset = false;

    /**
     * Partitions whose next message does not fit into the requested fetch size, as [topic][partition] => true
     *
     * @var array<string, array<int, bool>>
     */
    public array $oversizedMessages = [];

    /**
     * Members of the consumer group, member id => the `Subscription` bytes the member sent
     *
     * @var array<string, string>
     */
    public array $groupMembers = [];

    /**
     * Member id of the leader of the group, the first member that joined by default
     */
    public string $leaderId = '';

    /**
     * Generation of the group, incremented by every join, exactly as a coordinator does it
     */
    public int $generationId = 0;

    /**
     * Group protocol the members agreed on, the name of the assignor of the last join
     */
    public string $groupProtocol = '';

    /**
     * Assignment of every member as the leader published it, member id => the `MemberAssignment` bytes
     *
     * @var array<string, string>
     */
    public array $memberAssignments = [];

    /**
     * Partition ids of the topics the leader of a group assigns, [topic] => list of partition ids
     *
     * @var array<string, list<int>>
     */
    public array $partitionsPerTopic = [];

    /**
     * JoinGroup requests the consumer sent, in order
     *
     * @var list<array{groupId: string, memberId: string, protocolType: string, protocols: array<string, string>}>
     */
    public array $joins = [];

    /**
     * SyncGroup requests the consumer sent, in order
     *
     * @var list<array{groupId: string, memberId: string, generationId: int, assignments: array<string, string>}>
     */
    public array $syncs = [];

    /**
     * Heartbeat requests the consumer sent, in order
     *
     * @var list<array{groupId: string, memberId: string, generationId: int}>
     */
    public array $heartbeats = [];

    /**
     * LeaveGroup requests the consumer sent, in order
     *
     * @var list<array{groupId: string, memberId: string}>
     */
    public array $leaves = [];

    /**
     * Exceptions to throw on the next group requests, one per call, in order
     *
     * @var list<\Throwable|null>
     */
    public array $joinFailures = [];

    /**
     * @var list<\Throwable|null>
     */
    public array $syncFailures = [];

    /**
     * @var list<\Throwable|null>
     */
    public array $heartbeatFailures = [];

    /**
     * @var list<\Throwable|null>
     */
    public array $leaveFailures = [];

    /**
     * Exceptions to throw on the next commits, one per call, in order
     *
     * @var list<\Throwable|null>
     */
    public array $commitFailures = [];

    /**
     * Sequence of the member ids this coordinator hands out
     */
    private int $memberSequence = 0;

    public function __construct() {}

    /**
     * Adds records at the end of the log of a topic-partition
     *
     * @param list<string> $values Values of the records to append
     */
    public function append(string $topic, int $partition, array $values): void
    {
        $nextOffset = $this->logEndOffset($topic, $partition);
        foreach ($values as $value) {
            $this->log[$topic][$partition][] = new Record($value, null, 0, $nextOffset++);
        }
    }

    /**
     * @inheritdoc
     */
    public function fetchPartitions(array $topicPartitionOffsets, int $timeout): array
    {
        $this->fetchCalls[] = $topicPartitionOffsets;

        $failure = array_shift($this->fetchFailures);
        if ($failure !== null) {
            throw $failure;
        }

        $result = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partition => $offset) {
                $records = $this->log[$topic][$partition] ?? [];
                if (!$this->ignoreFetchOffset) {
                    $records = array_filter(
                        $records,
                        static fn(Record $record): bool => $record->offset >= $offset
                    );
                }

                $logEndOffset = $this->logEndOffset($topic, $partition);
                $isTooLarge   = ($this->oversizedMessages[$topic][$partition] ?? false) && $logEndOffset > $offset;

                $result[$topic][$partition] = new FetchedPartition(
                    new TopicPartition((string) $topic, (int) $partition),
                    (int) $offset,
                    KafkaException::NO_ERROR,
                    $logEndOffset,
                    $isTooLarge ? MessageSet::fromBuffer('') : self::messageSetOf(array_values($records)),
                    $isTooLarge
                );
            }
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    public function fetch(array $topicPartitionOffsets, int $timeout): array
    {
        $result = [];
        foreach ($this->fetchPartitions($topicPartitionOffsets, $timeout) as $topic => $partitions) {
            foreach ($partitions as $partition => $fetchedPartition) {
                $result[$topic][$partition] = $fetchedPartition->getRecords();
            }
        }

        return $result;
    }

    /**
     * Builds a message set that carries the given records at the offsets they already have
     *
     * MessageSet::fromRecords() numbers a produced set from 0, because the broker assigns the real offsets on
     * append; a fetched set has the offsets of the log, so its bytes are built to the specification here.
     *
     * @param list<Record> $records
     */
    private static function messageSetOf(array $records): MessageSet
    {
        $buffer = '';
        foreach ($records as $record) {
            $buffer .= SpecMessageSet::entry(
                (int) $record->offset,
                SpecMessageSet::message($record->key, $record->value, $record->attributes)
            );
        }

        return MessageSet::fromBuffer($buffer);
    }

    /**
     * @inheritdoc
     */
    public function fetchTopicPartitionOffsets(array $topicPartitionTimestamps): array
    {
        $result = [];
        foreach ($this->fetchTopicPartitionOffsetsForTimes($topicPartitionTimestamps) as $topic => $partitions) {
            foreach ($partitions as $partition => $found) {
                $result[$topic][$partition] = $found?->offset ?? OffsetsResponsePartition::UNKNOWN_OFFSET;
            }
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    public function fetchTopicPartitionOffsetsForTimes(array $topicPartitionTimestamps): array
    {
        $this->offsetsCalls[] = $topicPartitionTimestamps;

        $result = [];
        foreach ($topicPartitionTimestamps as $topic => $partitionTimes) {
            foreach ($partitionTimes as $partition => $time) {
                $result[$topic][$partition] = match ($time) {
                    // The two special values never read a message, so the broker answers them without a timestamp
                    OffsetsRequest::EARLIEST => new OffsetAndTimestamp(
                        $this->logStartOffset($topic, $partition),
                        OffsetsResponsePartition::UNKNOWN_TIMESTAMP
                    ),
                    OffsetsRequest::LATEST => new OffsetAndTimestamp(
                        $this->logEndOffset($topic, $partition),
                        OffsetsResponsePartition::UNKNOWN_TIMESTAMP
                    ),
                    default => $this->firstRecordAtOrAfter($topic, $partition, $time),
                };
            }
        }

        return $result;
    }

    /**
     * Finds the first record of a partition whose timestamp is at or after the given one, the way a 0.10.1 broker
     * resolves a timestamp through the time index of the log
     */
    private function firstRecordAtOrAfter(string $topic, int $partition, int $timestamp): ?OffsetAndTimestamp
    {
        foreach ($this->log[$topic][$partition] ?? [] as $record) {
            if ($record->timestamp !== null && $record->timestamp >= $timestamp) {
                return new OffsetAndTimestamp((int) $record->offset, $record->timestamp);
            }
        }

        // No message matches, which the broker reports with the offset -1 and no error at all
        return null;
    }

    /**
     * @inheritdoc
     */
    public function fetchGroupOffsets(Node $coordinatorNode, string $groupId, array $topicPartitions): array
    {
        $result = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $partitionIds = $partitions instanceof PartitionsForTopic ? $partitions->partitions : $partitions;
            foreach ($partitionIds as $partition) {
                $result[$topic][$partition] = $this->committedOffsets[$groupId][$topic][$partition] ?? -1;
            }
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    public function commitGroupOffsets(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        int $generationId,
        array $topicPartitionOffsets,
        int $retentionTimeMs
    ): void {
        $failure = array_shift($this->commitFailures);
        if ($failure !== null) {
            throw $failure;
        }

        $this->commits[] = [
            'group'         => $groupId,
            'memberId'      => $memberId,
            'generationId'  => $generationId,
            'offsets'       => $topicPartitionOffsets,
            'retentionTime' => $retentionTimeMs,
        ];

        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partition => $offset) {
                $this->committedOffsets[$groupId][$topic][$partition] = (int) (is_object($offset)
                    ? $offset->offset
                    : $offset);
            }
        }
    }

    /**
     * @inheritdoc
     */
    public function joinGroup(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        string $protocolType,
        array $groupProtocols
    ): JoinGroupResponse {
        $this->joins[] = [
            'groupId'      => $groupId,
            'memberId'     => $memberId,
            'protocolType' => $protocolType,
            'protocols'    => $groupProtocols,
        ];

        $failure = array_shift($this->joinFailures);
        if ($failure !== null) {
            throw $failure;
        }

        if ($memberId === JoinGroupRequest::DEFAULT_MEMBER_ID) {
            $memberId = 'member-' . ++$this->memberSequence;
        }

        $this->groupMembers[$memberId] = (string) reset($groupProtocols);
        $this->groupProtocol           = (string) key($groupProtocols);
        if (!isset($this->groupMembers[$this->leaderId])) {
            $this->leaderId = (string) array_key_first($this->groupMembers);
        }
        $this->generationId++;

        $response                = new JoinGroupResponse();
        $response->errorCode     = KafkaException::NO_ERROR;
        $response->generationId  = $this->generationId;
        $response->groupProtocol = $this->groupProtocol;
        $response->leaderId      = $this->leaderId;
        $response->memberId      = $memberId;
        $response->members       = [];

        if ($memberId === $this->leaderId) {
            foreach ($this->groupMembers as $groupMemberId => $metadata) {
                $response->members[$groupMemberId] = new JoinGroupResponseMember((string) $groupMemberId, $metadata);
            }
        }

        return $response;
    }

    /**
     * @inheritdoc
     */
    public function syncGroup(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        int $generationId,
        array $groupAssignments = []
    ): SyncGroupResponse {
        $this->syncs[] = [
            'groupId'      => $groupId,
            'memberId'     => $memberId,
            'generationId' => $generationId,
            'assignments'  => $groupAssignments,
        ];

        $failure = array_shift($this->syncFailures);
        if ($failure !== null) {
            throw $failure;
        }

        if ($groupAssignments !== []) {
            $this->memberAssignments = $groupAssignments;
        }

        $response                   = new SyncGroupResponse();
        $response->errorCode        = KafkaException::NO_ERROR;
        $response->memberAssignment = $this->memberAssignments[$memberId] ?? '';

        return $response;
    }

    /**
     * @inheritdoc
     */
    public function heartbeat(Node $coordinatorNode, string $groupId, string $memberId, int $generationId): void
    {
        $this->heartbeats[] = [
            'groupId'      => $groupId,
            'memberId'     => $memberId,
            'generationId' => $generationId,
        ];

        $failure = array_shift($this->heartbeatFailures);
        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * @inheritdoc
     */
    public function leaveGroup(Node $coordinatorNode, string $groupId, string $memberId): void
    {
        $this->leaves[] = ['groupId' => $groupId, 'memberId' => $memberId];

        unset($this->groupMembers[$memberId], $this->memberAssignments[$memberId]);

        $failure = array_shift($this->leaveFailures);
        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * Adds a member that is already in the group, so that a rebalance has more than this consumer to assign to
     *
     * @param list<string> $topics Topics the member subscribed to
     */
    public function addGroupMember(string $memberId, array $topics): void
    {
        $this->groupMembers[$memberId] = new Subscription($topics)->pack();
        if ($this->leaderId === '') {
            $this->leaderId = $memberId;
        }
    }

    /**
     * Returns the assignment the leader published for a member, as the plain partition lists of every topic
     *
     * @return array<string, list<int>>
     */
    public function assignmentOf(string $memberId): array
    {
        $assignment = $this->memberAssignments[$memberId] ?? '';

        return $assignment === '' ? [] : MemberAssignment::unpack($assignment)->partitions();
    }

    /**
     * @inheritdoc
     */
    public function getGroupCoordinator(string $groupId): Node
    {
        $node         = new Node();
        $node->nodeId = 1;
        $node->host   = 'fake-coordinator';
        $node->port   = 9092;

        return $node;
    }

    /**
     * Offset of the next record that would be appended to a topic-partition
     */
    private function logEndOffset(string $topic, int $partition): int
    {
        if (isset($this->logEndOffsets[$topic][$partition])) {
            return $this->logEndOffsets[$topic][$partition];
        }

        $records = $this->log[$topic][$partition] ?? [];
        if ($records === []) {
            return $this->logStartOffset($topic, $partition);
        }

        return (int) end($records)->offset + 1;
    }

    /**
     * First offset that is still available in a topic-partition
     */
    private function logStartOffset(string $topic, int $partition): int
    {
        if (isset($this->logStartOffsets[$topic][$partition])) {
            return $this->logStartOffsets[$topic][$partition];
        }

        $records = $this->log[$topic][$partition] ?? [];

        return $records === [] ? 0 : (int) $records[0]->offset;
    }
}
