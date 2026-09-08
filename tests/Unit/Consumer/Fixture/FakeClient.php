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
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
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
    public function fetchTopicPartitionOffsets(array $topicPartitions): array
    {
        $this->offsetsCalls[] = $topicPartitions;

        $result = [];
        foreach ($topicPartitions as $topic => $partitionTimes) {
            foreach ($partitionTimes as $partition => $time) {
                $result[$topic][$partition] = $time === OffsetsRequest::EARLIEST
                    ? $this->logStartOffset($topic, $partition)
                    : $this->logEndOffset($topic, $partition);
            }
        }

        return $result;
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
    public function commitGroupOffsets(Node $coordinatorNode, string $groupId, array $topicPartitionOffsets): void
    {
        $this->commits[] = ['group' => $groupId, 'offsets' => $topicPartitionOffsets];

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
