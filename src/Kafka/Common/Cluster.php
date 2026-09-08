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

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

/**
 * A representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 *
 * @TODO loadFromCache() method contains unsafe file inclusion, possible vector for attack
 */
final class Cluster
{
    /**
     * Fallback for `metadata.fetch.timeout.ms` when the configuration does not carry it
     */
    private const int DEFAULT_FETCH_TIMEOUT_MS = 60000;

    /**
     * Fallback for `metadata.max.age.ms` when the configuration does not carry it
     */
    private const int DEFAULT_MAX_AGE_MS = 300000;

    /**
     * Fallback for `retry.backoff.ms` when the configuration does not carry it
     */
    private const int DEFAULT_BACKOFF_MS = 100;

    /**
     * List of broker nodes, indexed by the node id
     *
     * @var array<int, Node>
     */
    private array $nodes = [];

    /**
     * Topic partitions, indexed by the topic name
     *
     * @var array<string, TopicMetadata>
     */
    private array $topicPartitions = [];

    /**
     * Point in time the metadata of this cluster was fetched at, as a unix timestamp in milliseconds
     */
    private int $fetchedAtMs = 0;

    /**
     * Creates a new cluster with the given nodes and partitions
     *
     * @param array<string, mixed> $configuration Client configuration
     */
    private function __construct(
        /**
         * Client configuration
         */
        private array $configuration
    ) {}

    /**
     * Creates a "bootstrap" cluster using the given list of host/ports.
     *
     * A broker that has just started answers a Metadata request with an EMPTY broker array, because its metadata
     * cache is only filled once the controller has pushed an `UpdateMetadata` request to it. That is "not ready
     * yet", never "the cluster has no brokers", so the metadata is requested again with `retry.backoff.ms` in
     * between until `metadata.fetch.timeout.ms` runs out.
     *
     * On a cluster that does not host a single topic yet the controller never publishes anything, and a Metadata
     * request with an empty topic list keeps answering with zero brokers indefinitely. Naming a topic breaks that
     * deadlock, because `auto.create.topics.enable` makes the broker create the topic and elect a leader for it;
     * a caller that knows which topic it is going to work with should therefore pass it.
     *
     * @param array<string, mixed> $configuration Broker client configuration
     * @param string|null          $topic         Topic to ask the metadata for, `null` asks for every topic
     *
     * @throws AllBrokersNotAvailableException If the cluster did not advertise a single broker in time
     *
     * @see docs/protocol/0.8.2.md, section "Cluster readiness"
     */
    public static function bootstrap(array $configuration, ?string $topic = null): Cluster
    {
        $cluster        = new Cluster($configuration);
        $isCacheEnabled = !empty($configuration[ClientConfig::METADATA_CACHE_FILE]);

        if ($isCacheEnabled && $cluster->loadFromCache()) {
            return $cluster;
        }

        $topics    = $topic !== null ? [$topic] : [];
        $timeoutMs = (int) ($configuration[ClientConfig::METADATA_FETCH_TIMEOUT_MS] ?? self::DEFAULT_FETCH_TIMEOUT_MS);
        $backoffMs = (int) ($configuration[ClientConfig::RETRY_BACKOFF_MS] ?? self::DEFAULT_BACKOFF_MS);
        $deadline  = microtime(true) + $timeoutMs / 1000;

        $attempts = 0;
        $causes   = [];
        do {
            $attempts++;
            try {
                $cluster->reload($topics);

                return $cluster;
            } catch (KafkaException $exception) {
                $causes[] = $exception->getMessage();
            }
            if ($backoffMs > 0) {
                usleep($backoffMs * 1000);
            }
        } while (microtime(true) < $deadline);

        throw new AllBrokersNotAvailableException(
            [
                'error'     => 'The cluster did not advertise any broker within the metadata fetch timeout',
                'timeoutMs' => $timeoutMs,
                'attempts'  => $attempts,
                'servers'   => $configuration[ClientConfig::BOOTSTRAP_SERVERS] ?? [],
                'cause'     => array_values(array_unique($causes)),
            ],
            KafkaException::BROKER_NOT_AVAILABLE
        );
    }

    /**
     * Gets the list of available partitions for this topic
     *
     * @param string $topic Name of the topic
     *
     * @return array<int, PartitionMetadata>
     */
    public function availablePartitionsForTopic(string $topic): array
    {
        return $this->partitionsForTopic($topic);
    }

    /**
     * Gets the current leader for the given topic-partition
     *
     * @throws UnknownTopicOrPartitionException If the topic-partition is unknown to the cluster
     */
    public function leaderFor(string $topic, int $partition): Node
    {
        $partitions = $this->partitionsForTopic($topic);
        if (!isset($partitions[$partition])) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        $metadata = $partitions[$partition];
        if ($metadata->partitionErrorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode(
                $metadata->partitionErrorCode,
                ['topic' => $topic, 'partition' => $partition]
            );
        }

        $leaderId = $metadata->leader;
        $leader   = $this->nodeById($leaderId);
        if ($leader === null) {
            throw new UnknownErrorException(
                [
                    'error'     => 'Can not find the node that leads this partition',
                    'topic'     => $topic,
                    'partition' => $partition,
                    'leader'    => $leaderId,
                    'nodes'     => array_keys($this->nodes),
                ]
            );
        }

        return $leader;
    }

    /**
     * Gets the node by the node id (or null if no such node exists)
     */
    public function nodeById(int $nodeId): ?Node
    {
        if (!isset($this->nodes[$nodeId])) {
            // The configuration of the cluster may have changed, give the metadata one chance to catch up
            $this->reloadQuietly();
        }

        return $this->nodes[$nodeId] ?? null;
    }

    /**
     * Returns the list of nodes in the cluster
     *
     * @return array<int, Node>
     */
    public function nodes(): array
    {
        $this->refreshIfStale();

        return $this->nodes;
    }

    /**
     * Gets the metadata for the specified partition
     *
     * @throws UnknownTopicOrPartitionException If the topic-partition is unknown to the cluster
     */
    public function partition(string $topic, int $partition): PartitionMetadata
    {
        $partitions = $this->partitionsForTopic($topic);
        if (!isset($partitions[$partition])) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        return $partitions[$partition];
    }

    /**
     * Gets the list of partitions for the specified topic
     *
     * @return array<int, PartitionMetadata>
     *
     * @throws InvalidTopicException If the cluster does not know this topic
     */
    public function partitionsForTopic(string $topic): array
    {
        $this->refreshIfStale();

        if (!isset($this->topicPartitions[$topic])) {
            $this->reloadQuietly();
            if (!isset($this->topicPartitions[$topic])) {
                throw new InvalidTopicException(['topic' => $topic]);
            }
        }

        $metadata = $this->topicPartitions[$topic];
        if ($metadata->topicErrorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($metadata->topicErrorCode, ['topic' => $topic]);
        }

        return $metadata->partitions;
    }

    /**
     * Reloads the metadata from the broker and optionally saves it in the cache.
     *
     * The bootstrap servers are tried in order until one of them answers; a broker that advertises an empty broker
     * list has not received the metadata of the cluster from the controller yet and is treated as unavailable, see
     * {@see Cluster::bootstrap()}.
     *
     * @param list<string> $topics Topics to ask the metadata for, an empty list asks for every topic
     *
     * @throws UnknownErrorException If not a single bootstrap server answered
     * @throws AllBrokersNotAvailableException If the cluster answered without advertising a broker
     */
    public function reload(array $topics = []): void
    {
        $brokerAddresses = $this->configuration[ClientConfig::BOOTSTRAP_SERVERS] ?? [];
        $clientId        = (string) ($this->configuration[ClientConfig::CLIENT_ID] ?? '');

        $metadata = null;
        $causes   = [];
        foreach ($brokerAddresses as $address) {
            try {
                $stream        = ConnectionFactory::open($address, $this->configuration);
                $correlationId = AbstractRequest::nextCorrelationId();
                new MetadataRequest($topics, $clientId, $correlationId)->writeTo($stream);

                $metadata = ResponseValidator::read(
                    MetadataResponse::class,
                    $stream,
                    $correlationId,
                    ['address' => $address]
                );
                break;
            } catch (NetworkException $exception) {
                // we ignore all network errors and just try the next one address
                $causes[$address] = $exception->getMessage();
                continue;
            }
        }
        if ($metadata === null) {
            throw new UnknownErrorException(
                [
                    'error' => 'Can not fetch information about cluster metadata',
                    'cause' => $causes,
                ]
            );
        }
        if ($metadata->brokers === []) {
            // An empty broker array means "the metadata cache of that broker is not filled yet", so the answer is
            // dropped instead of replacing the metadata this cluster already has
            throw new AllBrokersNotAvailableException(
                [
                    'error'  => 'The cluster did not advertise any broker yet',
                    'topics' => $topics,
                ],
                KafkaException::BROKER_NOT_AVAILABLE
            );
        }

        $this->fetchedAtMs     = (int) (microtime(true) * 1e3);
        $this->nodes           = $metadata->brokers;
        $this->topicPartitions = $metadata->topics;

        $isCacheEnabled = !empty($this->configuration[ClientConfig::METADATA_CACHE_FILE]);
        if ($isCacheEnabled) {
            $content   = '<?php return ' . var_export([$this->fetchedAtMs, $metadata], true) . ';';
            $cacheFile = $this->configuration[ClientConfig::METADATA_CACHE_FILE];
            file_put_contents($cacheFile, $content);
            if (function_exists('opcache_invalidate')) {
                opcache_invalidate($cacheFile, true);
            }
        }
    }

    /**
     * Get all topics
     *
     * @return list<string>
     */
    public function topics(): array
    {
        $this->refreshIfStale();

        return array_keys($this->topicPartitions);
    }

    /**
     * Returns the age of the metadata of this cluster, in milliseconds
     */
    public function getMetadataAgeMs(): int
    {
        return (int) (microtime(true) * 1e3) - $this->fetchedAtMs;
    }

    /**
     * Refreshes the metadata once it is older than `metadata.max.age.ms`.
     *
     * New brokers and new partitions of an existing topic are not announced to a client in any way, the only way to
     * discover them is to ask for the metadata again from time to time.
     */
    private function refreshIfStale(): void
    {
        $maxAgeMs = (int) ($this->configuration[ClientConfig::METADATA_MAX_AGE_MS] ?? self::DEFAULT_MAX_AGE_MS);
        if ($maxAgeMs <= 0 || $this->fetchedAtMs === 0 || $this->getMetadataAgeMs() < $maxAgeMs) {
            return;
        }

        $this->reloadQuietly();
    }

    /**
     * Reloads the metadata, keeping the metadata of the last successful attempt if the cluster does not answer.
     *
     * A periodic refresh and the lookup of an unknown node are both opportunistic: failing them would replace a
     * useful error further up the stack ("this topic does not exist") with a transport error.
     */
    private function reloadQuietly(): void
    {
        try {
            $this->reload();
        } catch (KafkaException) {
            // Keep whatever metadata is already known, the caller reports the real problem
        }
    }

    /**
     * Loads cluster configuration from the cache
     *
     * @return bool True if metadata was successfully loaded from the cache
     */
    private function loadFromCache(): bool
    {
        $milliSeconds = (int) (microtime(true) * 1e3);
        $cacheFile    = $this->configuration[ClientConfig::METADATA_CACHE_FILE];
        $maxAgeMs     = (int) ($this->configuration[ClientConfig::METADATA_MAX_AGE_MS] ?? self::DEFAULT_MAX_AGE_MS);
        if (is_readable($cacheFile)) {
            /** @var MetadataResponse $metadata */
            [$cachePutTimeMs, $metadata] = include $cacheFile;
            if (($milliSeconds - $cachePutTimeMs) < $maxAgeMs) {
                $this->fetchedAtMs     = $cachePutTimeMs;
                $this->nodes           = $metadata->brokers;
                $this->topicPartitions = $metadata->topics;

                return true;
            }
        }

        return false;
    }
}
