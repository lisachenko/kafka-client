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
/**
 * @author Alexander.Lisachenko
 * @date   29.07.2016
 */

namespace Protocol\Kafka\Consumer;

use BadMethodCallException;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * A Kafka client that consumes records from a Kafka 0.8.2.2 cluster.
 *
 * Kafka 0.8 has no broker-side group membership: the coordinator only stores committed offsets (GroupCoordinator,
 * OffsetCommit, OffsetFetch), while partition assignment and rebalancing were done by the consumers themselves
 * through ZooKeeper. This client therefore behaves like the 0.8 SimpleConsumer: partitions must be assigned
 * explicitly with assign(), and offsets can still be committed to and fetched from the broker.
 */
class KafkaConsumer
{
    /**
     * The consumer configs
     */
    private array $configuration;

    /**
     * Kafka cluster configuration
     *
     * @var Cluster
     */
    private $cluster;

    /**
     * Low-level kafka client
     *
     * @var Client
     */
    private $client;

    /**
     * List of assigned topic partitions
     */
    private array $assignedTopicPartitions = [];

    /**
     * List of paused topic partitions
     */
    private array $pausedTopicPartitions = [];

    /**
     * Offsets for topic partitions in the consumer group
     *
     * @var array
     */
    private $topicPartitionOffsets = [];

    /**
     * Offset coordinator node for the configured consumer group
     */
    private ?Node $coordinator = null;

    /**
     * Last commit time in ms
     */
    private ?int $lastAutoCommitMs = null;

    public function __construct(array $configuration = [])
    {
        $this->configuration = $configuration + ConsumerConfig::getDefaultConfiguration();
        $this->cluster       = Cluster::bootstrap($this->configuration);
        $this->client        = new Client($this->cluster, $this->configuration);
    }

    /**
     * Assign a list of partitions to this consumer.
     *
     * @param array $topicPartitions Key is topic and value is array of assigned partitions
     */
    public function assign(array $topicPartitions): void
    {
        if ($topicPartitions === []) {
            throw new \InvalidArgumentException(
                'Can not assign empty list of topic partitions to the consumer.' .
                'Probably, not enough partitions for this topic.'
            );
        }
        $this->assignedTopicPartitions = $topicPartitions;

        $topicPartitionOffsets = $this->client->fetchGroupOffsets(
            $this->getCoordinator(),
            $this->configuration[ConsumerConfig::GROUP_ID],
            $topicPartitions
        );
        $this->topicPartitionOffsets = $this->autoResetOffsets($topicPartitionOffsets);
    }

    /**
     * Get the set of topic partitions currently assigned to this consumer.
     *
     * @return array Key is topic and value is array of assigned partitions
     */
    public function assignment()
    {
        return $this->assignedTopicPartitions;
    }

    /**
     * Commit offsets returned on the last poll() for all the assigned list of topics and partitions.
     *
     * @param array $topicPartitionOffsets Specified offsets for the specified list of topics and partitions.
     */
    public function commitSync(?array $topicPartitionOffsets = null): void
    {
        $topicPartitionOffsets ??= $this->topicPartitionOffsets;

        $this->client->commitGroupOffsets(
            $this->getCoordinator(),
            $this->configuration[ConsumerConfig::GROUP_ID],
            $topicPartitionOffsets
        );

        $this->topicPartitionOffsets = $topicPartitionOffsets;
    }

    /**
     * Gets the partition metadata for the given topic.
     *
     * @param string $topic
     *
     * @return PartitionMetadata[]
     */
    public function partitionsFor($topic)
    {
        return $this->cluster->partitionsForTopic($topic);
    }

    /**
     * Suspend fetching from the requested partitions.
     *
     * @param array $topicPartitions List of topic partitions to suspend
     */
    public function pause(array $topicPartitions): void
    {
        $this->pausedTopicPartitions = $topicPartitions;
    }

    /**
     * Fetches data for the topics or partitions specified using the assign() API.
     *
     * It is an error to not have assigned any topics or partitions before polling for data.
     *
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially.
     * The last consumed offset can be manually set through seek(topic, partition, long) or automatically set as the
     * last committed offset for the assigned list of partitions
     *
     * @param integer $timeout The time, in milliseconds, spent waiting in poll if data is not available.
     *                         If 0, returns immediately with any records that are available now.
     */
    public function poll($timeout)
    {
        $milliSeconds = (int) (microtime(true) * 1e3);

        $activeTopicPartitionOffsets = $this->topicPartitionOffsets;
        foreach ($this->pausedTopicPartitions as $topic => $partitions) {
            // This can be optimized in pause()/resume methods
            $activeTopicPartitionOffsets[$topic] = array_diff($activeTopicPartitionOffsets[$topic], $partitions);
        }
        $result = $this->client->fetch($activeTopicPartitionOffsets, $timeout);

        $resultOffsets = $this->fetchResultOffsets($result);
        if ($resultOffsets) {
            $this->topicPartitionOffsets = array_replace_recursive($this->topicPartitionOffsets, $resultOffsets);
        }

        if ($this->configuration[ConsumerConfig::ENABLE_AUTO_COMMIT]) {
            if (($milliSeconds - $this->lastAutoCommitMs) > $this->configuration[ConsumerConfig::AUTO_COMMIT_INTERVAL_MS]) {
                $this->commitSync();
                $this->lastAutoCommitMs = $milliSeconds;
            }
        }

        return $result;
    }

    /**
     * Get the offset of the next record that will be fetched (if a record with that offset exists).
     *
     * @param string $topic Name of the topic
     * @param integer $partition Id of partition
     *
     * @return integer
     */
    public function position($topic, $partition): int|float
    {
        if (!isset($this->assignedTopicPartitions[$topic][$partition])) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        return $this->topicPartitionOffsets[$topic][$partition] + 1;
    }

    /**
     * Resume specified partitions which have been paused with pause($topicPartitions).
     *
     * @param array $topicPartitions List of topic partitions to resume
     */
    public function resume(array $topicPartitions): void
    {
        foreach ($topicPartitions as $topic => $partitions) {
            if (isset($this->pausedTopicPartitions[$topic])) {
                $this->pausedTopicPartitions[$topic] = array_diff($this->pausedTopicPartitions['topic'], $partitions);
            }
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll(timeout).
     *
     * @param string $topic Name of the topic
     * @param integer $partition Id of partition
     * @param integer $offset New offset value
     */
    public function seek($topic, $partition, $offset): void
    {
        if (!isset($this->assignedTopicPartitions[$topic][$partition])) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }
        $this->topicPartitionOffsets[$topic][$partition] = $offset;
    }

    /**
     * Seek to the first offset for each of the given partitions.
     *
     * @param array $topicPartitions
     */
    public function seekToBeginning(array $topicPartitions): void
    {
        $this->fetchOffsetAndSeek($topicPartitions, OffsetsRequest::EARLIEST);
    }

    /**
     * Seek to the last offset for each of the given partitions.
     *
     * @param array $topicPartitions
     */
    public function seekToEnd(array $topicPartitions): void
    {
        $this->fetchOffsetAndSeek($topicPartitions, OffsetsRequest::LATEST);
    }

    /**
     * Subscribing to topics is not available on the Kafka 0.8 protocol line.
     *
     * Dynamic partition assignment requires the broker-side group membership protocol (API keys 11-14), which was
     * introduced in Kafka 0.9; a 0.8.2.2 broker does not serve those API keys. Assign the partitions explicitly
     * with assign() instead.
     *
     * @param array $topics List of topics to subscribe
     */
    public function subscribe(array $topics): void
    {
        throw new BadMethodCallException(
            'Kafka 0.8 has no broker-side group membership, so subscribe() can not be supported: the group '
            . 'membership APIs (keys 11-14) only exist since Kafka 0.9, and a 0.8.2.2 broker either answers them '
            . 'with "Unknown api code" or closes the connection. Use assign() to select the topic partitions for '
            . 'this consumer explicitly.'
        );
    }

    /**
     * Clears any partitions directly assigned through assign(array $topicPartitions).
     */
    public function unsubscribe(): void
    {
        $this->assignedTopicPartitions = [];
        $this->topicPartitionOffsets   = [];
    }

    /**
     * Returns the offset coordinator for the configured consumer group, looking it up on the first use.
     */
    protected function getCoordinator(): Node
    {
        return $this->coordinator ??= $this->client->getGroupCoordinator(
            $this->configuration[ConsumerConfig::GROUP_ID]
        );
    }

    /**
     * Verifies fetched partitions and asks broker for the latest/earlisest offsets or throws an exception
     *
     * @param array $topicPartitionOffsets List of topic partitions
     *
     * @return array Existing or adjusted offsets (reloaded from the Kafka)
     */
    protected function autoResetOffsets(array $topicPartitionOffsets): array
    {
        $result = $topicPartitionOffsets;

        $unknownTopicPartitions = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            $unknownPartitionOffsets = array_keys($partitionOffsets, -1, true);
            if ($unknownPartitionOffsets !== []) {
                $unknownTopicPartitions[$topic] = $unknownPartitionOffsets;
            }
        }
        if ($unknownTopicPartitions === []) {
            return $result;
        }
        $fetchedOffsets = match ($this->configuration[ConsumerConfig::AUTO_OFFSET_RESET]) {
            OffsetResetStrategy::LATEST => $this->fetchOffsetAndSeek($unknownTopicPartitions, OffsetsRequest::LATEST),
            OffsetResetStrategy::EARLIEST => $this->fetchOffsetAndSeek($unknownTopicPartitions, OffsetsRequest::EARLIEST),
            default => throw new OffsetOutOfRangeException(['unknownTopicPartitions' => $unknownTopicPartitions]),
        };

        return array_replace_recursive($topicPartitionOffsets, $fetchedOffsets);
    }

    /**
     * Fetches offsets for specific topics and partitions
     *
     * @param array   $topicPartitions List of topic and partitions
     * @param integer $requestType     Offset type, e.g. OffsetsRequest::EARLIEST
     *
     * @return array
     */
    protected function fetchOffsetAndSeek(array $topicPartitions, $requestType)
    {
        $topicPartitionOffsetsRequest = [];

        $unknownTopics = array_diff_key($topicPartitions, $this->assignedTopicPartitions);
        if ($unknownTopics !== []) {
            throw new UnknownTopicOrPartitionException(['unknownTopics' => $unknownTopics]);
        }
        foreach ($topicPartitions as $topic => $partitions) {
            $unknownPartitions = array_diff($partitions, $this->assignedTopicPartitions[$topic]);
            if ($unknownPartitions !== []) {
                throw new UnknownTopicOrPartitionException(['topic' => $topic, 'unknownPartitions' => $unknownPartitions]);
            }
            $topicPartitionOffsetsRequest[$topic] = array_fill_keys($partitions, $requestType);
        }
        $topicPartitionOffsets = $this->client->fetchTopicPartitionOffsets($topicPartitionOffsetsRequest);

        return $topicPartitionOffsets;
    }

    /**
     * This methods looks for the offsets in the returned MessageSets and returns them incremented
     *
     * @param array $fetchResult Result from FetchResponse->topics
     *
     * @return array Last offsets, returned from the poll()
     */
    protected function fetchResultOffsets(array $fetchResult): array
    {
        $result = [];

        foreach ($fetchResult as $topic => $partitions) {
            foreach ($partitions as $partitionId => $messageSet) {
                if (empty($messageSet)) {
                    continue;
                }
                /** @var RecordBatch $lastMessage */
                $lastMessage = end($messageSet);
                $result[$topic][$partitionId] = $lastMessage->offset + 1;
            }
        }

        return $result;
    }
}
