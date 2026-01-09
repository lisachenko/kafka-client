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

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * A Kafka client that consumes records from a Kafka cluster.
 */
class KafkaConsumer
{
    /**
     * The producer configs
     */
    private array $configuration;

    /**
     * Kafka cluster configuration
     *
     * @var Cluster
     */
    private $cluster;

    /**
     * Assignor strategy
     */
    private readonly PartitionAssignorInterface $assignorStrategy;

    /**
     * Low-level kafka client
     *
     * @var Client
     */
    private $client;

    /**
     * Assigned memberId for this consumer
     *
     * @var string
     */
    private $memberId;

    /**
     * Assigned consumer generation ID
     *
     * @var integer
     */
    private $generationId;

    /**
     * Metadata for subscribed topics
     *
     * @var Subscription
     */
    private $subscription;

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
     * Coordinator node
     *
     * @var Node
     */
    private $coordinator;

    /**
     * Last hearbeat time in ms
     *
     * @var integer
     */
    private $lastHearbeatMs;

    /**
     * Default configuration for producer
     */
    private static array $defaultConfiguration = [
        /* Used configs */
        ConsumerConfig::BOOTSTRAP_SERVERS             => [],
        ConsumerConfig::CLIENT_ID                     => 'PHP/Kafka',
        ConsumerConfig::GROUP_ID                      => '',
        ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => RoundRobinAssignor::class,
        ConsumerConfig::SESSION_TIMEOUT_MS            => 30000,
        ConsumerConfig::FETCH_MIN_BYTES               => 1,
        ConsumerConfig::FETCH_MAX_WAIT_MS             => 500,
        ConsumerConfig::MAX_PARTITION_FETCH_BYTES     => 65536,
        ConsumerConfig::AUTO_OFFSET_RESET             => OffsetResetStrategy::LATEST,
        ConsumerConfig::REQUEST_TIMEOUT_MS            => 2000,
        ConsumerConfig::HEARTBEAT_INTERVAL_MS         => 1000,


        ConsumerConfig::SSL_KEY_PASSWORD          => null,
        ConsumerConfig::SSL_KEYSTORE_LOCATION     => null,
        ConsumerConfig::SSL_KEYSTORE_PASSWORD     => null,
        ConsumerConfig::CONNECTIONS_MAX_IDLE_MS   => 540000,
        ConsumerConfig::RECEIVE_BUFFER_BYTES      => 32768,
        ConsumerConfig::REQUEST_TIMEOUT_MS        => 30000,
        ConsumerConfig::SASL_MECHANISM            => 'GSSAPI',
        ConsumerConfig::SECURITY_PROTOCOL         => 'plaintext',
        ConsumerConfig::SEND_BUFFER_BYTES         => 131072,
        ConsumerConfig::METADATA_MAX_AGE_MS       => 300000,
        ConsumerConfig::RECONNECT_BACKOFF_MS      => 50,
        ConsumerConfig::RETRY_BACKOFF_MS          => 100,
    ];

    public function __construct(array $configuration = [])
    {
        $this->configuration = $configuration + self::$defaultConfiguration;
        $this->cluster       = Cluster::bootstrap($this->configuration[ConsumerConfig::BOOTSTRAP_SERVERS]);
        $this->client        = new Client($this->cluster, $this->configuration);
        $assignorStrategy    = $this->configuration[ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY];

        if (!is_subclass_of($assignorStrategy, PartitionAssignorInterface::class)) {
            throw new \InvalidArgumentException("Partition strategy class should implement PartitionAssignorInterface");
        }
        $this->assignorStrategy = new $assignorStrategy();
    }

    /**
     * Assign a list of partitions to this consumer.
     *
     * @param array $topicPartitions Key is topic and value is array of assigned partitions
     */
    public function assign(array $topicPartitions): void
    {
        $unknownTopics = array_diff($this->subscription->topics, array_keys($topicPartitions));
        if ($unknownTopics !== []) {
            $unknownTopics = implode(', ', $unknownTopics);
            throw new UnknownTopicOrPartitionException("Can not set partitions for non-subscribed topics: {$unknownTopics}");
        }
        $this->assignedTopicPartitions = $topicPartitions;

        $topicPartitionOffsets = $this->client->fetchOffsets(
            $this->coordinator,
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
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partitions.
     *
     * @param array $topicPartitionOffsets Specified offsets for the specified list of topics and partitions.
     */
    public function commitSync(?array $topicPartitionOffsets = null): void
    {
        $topicPartitionOffsets ??= $this->topicPartitionOffsets;

        $this->client->commitOffsets(
            $this->coordinator,
            $this->configuration[ConsumerConfig::GROUP_ID],
            $topicPartitionOffsets
        );

        // TODO: update current value of $this->topicPartitionOffsets
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
     * Fetches data for the topics or partitions specified using one of the subscribe/assign APIs.
     *
     * It is an error to not have subscribed to any topics or partitions before polling for data.
     *
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially.
     * The last consumed offset can be manually set through seek(topic, partition, long) or automatically set as the
     * last committed offset for the subscribed list of partitions
     *
     * @param integer $timeout The time, in milliseconds, spent waiting in poll if data is not available.
     *                         If 0, returns immediately with any records that are available now.
     */
    public function poll($timeout)
    {
        $milliSeconds = (int) (microtime(true) * 1e3);
        if (($milliSeconds - $this->lastHearbeatMs) > $this->configuration[ConsumerConfig::HEARTBEAT_INTERVAL_MS]) {
            $this->heartbeat($milliSeconds);
        }

        $activeTopicPartitionOffsets = $this->topicPartitionOffsets;
        foreach ($this->pausedTopicPartitions as $topic => $partitions) {
            $activeTopicPartitionOffsets[$topic] = array_diff($activeTopicPartitionOffsets[$topic], $partitions);
        }
        $result = $this->client->fetch($activeTopicPartitionOffsets, $timeout);

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
            throw new UnknownTopicOrPartitionException("Consumer was not assigned to the {$topic}:{$partition}");
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
            throw new UnknownTopicOrPartitionException("Consumer was not assigned to the {$topic}:{$partition}");
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
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     *
     * @param array $topics List of topics to subscribe
     */
    public function subscribe(array $topics): void
    {
        $groupId           = $this->configuration[ConsumerConfig::GROUP_ID];
        $this->coordinator = $this->client->getGroupCoordinator($groupId);

        $subscription = Subscription::fromSubscription($topics);
        $joinResult   = $this->client->joinGroup(
            $this->coordinator,
            $this->configuration[ConsumerConfig::GROUP_ID],
            $this->memberId,
            'consumer',
            ['range' => $subscription]
        );

        $this->memberId     = $joinResult->memberId;
        $this->generationId = $joinResult->generationId;

        $isLeader = $joinResult->memberId === $joinResult->leaderId;

        if ($isLeader) {
            $groupAssignments = $this->assignorStrategy->assign($this->cluster, $joinResult->members);
            $syncResult       = $this->client->syncGroup(
                $this->coordinator,
                $this->configuration[ConsumerConfig::GROUP_ID],
                $this->memberId,
                $this->generationId,
                $groupAssignments
            );
            $topicPartitions = $groupAssignments[$this->memberId]->topicPartitions;
        } else {
            $syncResult = $this->client->syncGroup(
                $this->coordinator,
                $this->configuration[ConsumerConfig::GROUP_ID],
                $this->memberId,
                $this->generationId
            );

            $assignments = MemberAssignment::unpack(new StringStream($syncResult->memberAssignment));

            // TODO: Use $assignments->userData; $assignments->version;
            $topicPartitions = $assignments->topicPartitions;
        }
        $this->subscription = $subscription;
        $this->assign($topicPartitions);
    }

    /**
     * Get the current subscription
     *
     * @return Subscription
     */
    public function subscription()
    {
        return $this->subscription;
    }

    /**
     * Unsubscribes from topics currently subscribed with subscribe(array $topics).
     *
     * This also clears any partitions directly assigned through assign(array $topicPartitions).
     */
    public function unsubscribe(): void
    {
        $result = $this->client->leaveGroup(
            $this->coordinator,
            $this->configuration[ConsumerConfig::GROUP_ID],
            $this->memberId
        );
        unset($this->subscription);
        $this->assignedTopicPartitions = [];
        $this->topicPartitionOffsets   = [];
    }

    /**
     * Performs a heartbeat for the group
     *
     * @param int $heartBeatTimeMs timestamp in ms (microtime(true) * 100)
     */
    protected function heartbeat($heartBeatTimeMs)
    {
        try {
            $this->client->heartbeat(
                $this->coordinator,
                $this->configuration[ConsumerConfig::GROUP_ID],
                $this->memberId,
                $this->generationId
            );
        } catch (KafkaException) {
            // Re-subscribe to the group in the case of failed heartbeat
            $this->subscribe($this->subscription->topics);
        }
        $this->lastHearbeatMs = $heartBeatTimeMs; // Expect 64-bit platform PHP
    }

    /**
     * Verifies fetched partitions and asks broker for the latest/earlisest offsets or throws an exception
     *
     * @param array $topicPartitionOffsets List of topic partitions
     *
     * @return array Existing or adjusted offsets (reloaded from the Kafka)
     */
    protected function autoResetOffsets(array $topicPartitionOffsets)
    {
        $unknownTopicPartitions = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            $unknownPartitionOffsets = array_keys($partitionOffsets, -1, true);
            if ($unknownPartitionOffsets !== []) {
                $unknownTopicPartitions[$topic] = $unknownPartitionOffsets;
            }
        }
        if ($unknownTopicPartitions === []) {
            return $this->topicPartitionOffsets;
        }
        return match ($this->configuration[ConsumerConfig::AUTO_OFFSET_RESET]) {
            'latest' => $this->fetchOffsetAndSeek($unknownTopicPartitions, OffsetsRequest::LATEST),
            'earliest' => $this->fetchOffsetAndSeek($unknownTopicPartitions, OffsetsRequest::EARLIEST),
            default => throw new OffsetOutOfRangeException("Can not reliable determine consumer partition offsets"),
        };
    }

    /**
     * Fetches offsets for specific topics and partitions
     *
     * @param array   $topicPartitions List of topic and partitions
     * @param integer $requestType     Offset type, e.g. OffsetsRequest::EARLIEST
     *
     * @return array
     */
    protected function fetchOffsetAndSeek(array $topicPartitions, $requestType): array
    {
        $topicPartitionOffsetsRequest = [];

        $unknownTopics = array_diff_key($topicPartitions, $this->assignedTopicPartitions);
        if ($unknownTopics !== []) {
            $unknownTopics = implode(', ', $unknownTopics);
            throw new UnknownTopicOrPartitionException('Consumer was not assigned to the ' . $unknownTopics . ' topics');
        }
        foreach ($topicPartitions as $topic => $partitions) {
            $unknownPartitions = array_diff($partitions, $this->assignedTopicPartitions[$topic]);
            if ($unknownPartitions !== []) {
                $partitionsString = implode(', ', $unknownPartitions);
                throw new UnknownTopicOrPartitionException("Consumer was not assigned to the {$topic}:{$partitionsString}");
            }
            $topicPartitionOffsetsRequest[$topic] = array_fill_keys($partitions, $requestType);
        }
        $topicPartitionOffsets = $this->client->offsets($topicPartitionOffsetsRequest);

        return array_replace_recursive($this->topicPartitionOffsets, $topicPartitionOffsets);
    }
}
