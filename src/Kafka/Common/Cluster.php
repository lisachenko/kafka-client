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
     * Configuration key that hides the internal topics of Kafka from {@see self::topics()}
     *
     * The key is the `exclude.internal.topics` of the consumer, declared as
     * {@see \Protocol\Kafka\Consumer\ConsumerConfig::EXCLUDE_INTERNAL_TOPICS}; it is spelled out here so that the
     * Common layer does not have to depend on the Consumer one. It defaults to `false`, i.e. `topics()` lists
     * every topic the cluster answered with, as it always did.
     */
    private const string EXCLUDE_INTERNAL_TOPICS = 'exclude.internal.topics';

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
     * Identifier of the cluster, null while the metadata was never fetched or came from a broker without one
     *
     * @since Version 2 of the Metadata API (Kafka 0.10.1)
     */
    private ?string $clusterId = null;

    /**
     * Broker id of the active controller, null while the metadata was never fetched, -1 while no broker leads
     *
     * @since Version 1 of the Metadata API (Kafka 0.10.0)
     */
    private ?int $controllerId = null;

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
     * On the lines below 0.10 that answer never came at all on a cluster without a single topic, because the
     * controller published nothing before the first topic existed; naming a topic broke the deadlock, since
     * `auto.create.topics.enable` makes the broker create it and elect a leader for it. A 0.10.2.2 broker fills
     * its cache with the alive brokers as soon as the controller has elected itself, so naming a topic is no
     * longer needed for the bootstrap - it still saves the caller the metadata of every topic of the cluster.
     *
     * The metadata is asked for with version 2 of the api, i.e. with a `null` topic array when no topic is named.
     *
     * @param array<string, mixed> $configuration Broker client configuration
     * @param string|null          $topic         Topic to ask the metadata for, `null` asks for every topic
     *
     * @throws AllBrokersNotAvailableException If the cluster did not advertise a single broker in time
     *
     * @see docs/protocol/0.10.2.md, section "Cluster readiness"
     */
    public static function bootstrap(array $configuration, ?string $topic = null): Cluster
    {
        $cluster        = new Cluster($configuration);
        $isCacheEnabled = !empty($configuration[ClientConfig::METADATA_CACHE_FILE]);

        if ($isCacheEnabled && $cluster->loadFromCache()) {
            return $cluster;
        }

        // A null topic list is the "every topic" of Metadata v1 and above; an EMPTY list would ask for no topic
        $topics    = $topic !== null ? [$topic] : null;
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
     * The request is version 2 of the Metadata API, so `null` asks for every topic of the cluster and an EMPTY
     * list asks for none of them - two intentions that version 0 had to express with the same empty array.
     *
     * @param list<string>|null $topics Topics to ask the metadata for, `null` asks for every topic
     *
     * @throws UnknownErrorException If not a single bootstrap server answered
     * @throws AllBrokersNotAvailableException If the cluster answered without advertising a broker
     */
    public function reload(?array $topics = null): void
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
        $this->clusterId       = $metadata->clusterId;
        $this->controllerId    = $metadata->controllerId;

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
     * A topic is internal when Kafka itself keeps it - `__consumer_offsets`, the log of the committed offsets - and
     * the answer says so from version 1 of the Metadata API on ({@see TopicMetadata::$isInternal}). Whether they
     * are listed here follows the `exclude.internal.topics` of the configuration by default, so a consumer that
     * sets the option never sees them; an explicit argument overrules it.
     *
     * @param bool|null $excludeInternalTopics Hide the internal topics, `null` follows `exclude.internal.topics`
     *
     * @return list<string>
     */
    public function topics(?bool $excludeInternalTopics = null): array
    {
        $this->refreshIfStale();

        $excludeInternalTopics ??= (bool) ($this->configuration[self::EXCLUDE_INTERNAL_TOPICS] ?? false);
        if (!$excludeInternalTopics) {
            return array_keys($this->topicPartitions);
        }

        $topics = array_filter(
            $this->topicPartitions,
            static fn(TopicMetadata $metadata): bool => $metadata->isInternal !== true
        );

        return array_keys($topics);
    }

    /**
     * Returns the identifier of this cluster, or null when the brokers do not have one
     *
     * The cluster id is generated by the first broker of a cluster that runs Kafka 0.10.1 or newer and stored in
     * ZooKeeper (`KafkaServer.getOrGenerateClusterId` @ 0.10.2.2), so every broker of the cluster answers the same
     * 22 character string. It only travels in version 2 of the Metadata API and is therefore null for a cluster
     * whose metadata was read with an older version.
     */
    public function clusterId(): ?string
    {
        $this->refreshIfStale();

        return $this->clusterId;
    }

    /**
     * Returns the broker that is the active controller of the cluster, or null while there is none
     *
     * The controller is the broker that elects the partition leaders and the only one that serves the topic
     * administration apis ({@see \Protocol\Kafka\Admin\AdminClient::findController()}). Its id arrives with every
     * Metadata answer from version 1 on; `-1` is what a broker reports while the cluster is electing a new
     * controller, and it is answered here as null, exactly like a controller that is not among the alive brokers.
     */
    public function controller(): ?Node
    {
        $this->refreshIfStale();

        if ($this->controllerId === null || $this->controllerId === MetadataResponse::NO_CONTROLLER_ID) {
            return null;
        }

        return $this->nodeById($this->controllerId);
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
                // A file that was written before this client knew the fields of Metadata v1 and v2 has neither
                $this->clusterId    = $metadata->clusterId ?? null;
                $this->controllerId = $metadata->controllerId ?? null;

                return true;
            }
        }

        return false;
    }
}
