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
 * @date 02.08.2016
 */

namespace Protocol\Kafka;

use Closure;
use Exception;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig as ConsumerConfig;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\ProducerConfig as ProducerConfig;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

/**
 * Low-level client for the Kafka 0.8.2.2 protocol.
 *
 * Kafka 0.8 has no broker-side group membership (the API keys 11-14 were only added in 0.9), therefore this client
 * only speaks Produce, Fetch, Offsets, OffsetCommit, OffsetFetch and GroupCoordinator (ConsumerMetadata in 0.8.2).
 *
 * Every request that addresses topic-partitions is split by their current leader and sent to all of those brokers
 * at once; the answers are collected with `stream_select()` as they arrive. A topic-partition whose leader answered
 * with a retriable error - 3 UnknownTopicOrPartition, 5 LeaderNotAvailable, 6 NotLeaderForPartition - or whose
 * connection dropped is sent again after the cluster metadata has been refreshed, up to `retries` times. What is
 * still broken afterwards is reported as a {@see TopicPartitionRequestException} that carries both the partial
 * result of the partitions that did succeed and the error of each partition that did not.
 *
 * @see docs/protocol/0.8.2.md
 */
class Client
{
    /**
     * Fallback for `request.timeout.ms` when the configuration does not carry it
     */
    private const int DEFAULT_REQUEST_TIMEOUT_MS = 30000;

    public function __construct(
        /**
         * Cluster configuration
         */
        private readonly Cluster $cluster,
        /**
         * Client configuration
         */
        private array $configuration = []
    ) {}

    /**
     * Produce messages to the specific topic partition
     *
     * @param array<string, array<int, iterable<Record|string|\Stringable>>> $topicPartitionMessages Messages for
     *        each topic and partition
     *
     * @return array<string, array<int, ProduceResponsePartition>> Accepted partitions in the form
     *         [topic => [partition => ProduceResponsePartition]], empty for a fire-and-forget request (acks = 0),
     *         which the broker never answers
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     * @throws InvalidConfigurationException  For a `compression.type` that this client can not write
     */
    public function produce(array $topicPartitionMessages): array
    {
        $requiredAcks = (int) $this->configuration[ProducerConfig::ACKS];

        // `compression.type` compresses a whole batch at once, so it is applied per topic-partition, not per record
        $compressionCodec = ProducerConfig::compressionCodec(
            $this->configuration[ProducerConfig::COMPRESSION_TYPE] ?? ProducerConfig::COMPRESSION_TYPE_NONE
        );

        // The wire format carries one opaque message set per topic-partition, see docs/protocol/0.8.2.md
        $topicPartitionMessageSets = [];
        foreach ($topicPartitionMessages as $topic => $partitionMessages) {
            foreach ($partitionMessages as $partition => $messages) {
                $topicPartitionMessageSets[$topic][$partition] = MessageSet::fromRecords(
                    self::toRecords($messages),
                    $compressionCodec
                );
            }
        }

        $createRequest = fn(array $nodeTopicPartitionMessageSets, int $correlationId): ProduceRequest
            => new ProduceRequest(
                $nodeTopicPartitionMessageSets,
                $requiredAcks,
                $this->configuration[ProducerConfig::TIMEOUT_MS],
                $this->configuration[ProducerConfig::CLIENT_ID],
                $correlationId
            );

        // acks = 0 is the only request of the protocol that the broker does not answer, so nothing may be read back
        // from those connections, see ProduceRequest::expectsResponse()
        if ($requiredAcks === ProduceRequest::ACKS_NONE) {
            $this->fireAndForget($topicPartitionMessageSets, $createRequest);

            return [];
        }

        return $this->clusterRequest(
            $topicPartitionMessageSets,
            $createRequest,
            ProduceResponse::class,
            static function (array $result, ProduceResponse $response, array &$errors): array {
                foreach ($response->topics as $topic => $topicResult) {
                    /** @var ProduceResponsePartition[] $partitions */
                    $partitions = $topicResult->partitions;
                    foreach ($partitions as $partitionId => $partitionInfo) {
                        if ($partitionInfo->errorCode !== KafkaException::NO_ERROR) {
                            $errors[$topic][$partitionId] = KafkaException::fromCode(
                                $partitionInfo->errorCode,
                                ['topic' => $topic, 'partitionId' => $partitionId]
                            );
                            continue;
                        }
                        $result[$topic][$partitionId] = $partitionInfo;
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Fetches messages from the specified topic and partitions
     *
     * @param array<string, array<int, int>> $topicPartitionOffsets Offsets to start fetching each partition at
     * @param int                            $timeout               Timeout in ms to wait for fetching
     *
     * @return array<string, array<int, list<Record>>> Records in the form [topic => [partition => Record[]]]
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions, the
     *         partial result of it carries the records of the partitions that did answer
     */
    public function fetch(array $topicPartitionOffsets, int $timeout): array
    {
        try {
            return self::toRecordsByPartition($this->fetchPartitions($topicPartitionOffsets, $timeout));
        } catch (TopicPartitionRequestException $exception) {
            throw new TopicPartitionRequestException(
                self::toRecordsByPartition($exception->getPartialResult()),
                $exception->getExceptions()
            );
        }
    }

    /**
     * Fetches messages together with the state of each topic-partition they came from.
     *
     * A consumer needs more than the records to drive its fetch loop: the high water mark of a partition tells it
     * how far behind the end of the log it is, and a message that is larger than `max.partition.fetch.bytes` makes
     * a 0.8.2.2 broker answer without an error and without a single complete message, which would turn a naive
     * fetch loop into an endless one, see {@see FetchedPartition::isSingleMessageTooLarge()}.
     *
     * @param array<string, array<int, int>> $topicPartitionOffsets Offsets to start fetching each partition at
     * @param int                            $timeout               Timeout in ms to wait for fetching
     *
     * @return array<string, array<int, FetchedPartition>> [topic => [partition => FetchedPartition]]
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     */
    public function fetchPartitions(array $topicPartitionOffsets, int $timeout): array
    {
        $timeout = (int) min($this->configuration[ConsumerConfig::FETCH_MAX_WAIT_MS], $timeout);
        // A consumer that trusts its network may skip the checksum of every single message it reads
        $checkCrcs = (bool) ($this->configuration[ConsumerConfig::CHECK_CRCS] ?? true);

        return $this->clusterRequest(
            $topicPartitionOffsets,
            fn(array $nodeTopicRequest, int $correlationId): FetchRequest => new FetchRequest(
                $nodeTopicRequest,
                $timeout,
                $this->configuration[ConsumerConfig::FETCH_MIN_BYTES],
                $this->configuration[ConsumerConfig::MAX_PARTITION_FETCH_BYTES],
                -1,
                $this->configuration[ConsumerConfig::CLIENT_ID],
                $correlationId
            ),
            FetchResponse::class,
            static function (array $result, FetchResponse $response, array &$errors) use (
                $topicPartitionOffsets,
                $checkCrcs
            ): array {
                foreach ($response->topics as $topic => $topicResponse) {
                    /** @var FetchResponsePartition $responsePartition */
                    foreach ($topicResponse->partitions as $partitionId => $responsePartition) {
                        if ($responsePartition->errorCode !== KafkaException::NO_ERROR) {
                            $errors[$topic][$partitionId] = KafkaException::fromCode(
                                $responsePartition->errorCode,
                                ['topic' => $topic, 'partitionId' => $partitionId]
                            );
                            continue;
                        }
                        $fetchOffset = (int) ($topicPartitionOffsets[$topic][$partitionId] ?? 0);
                        try {
                            // The schema engine hands over the raw bytes of the message set, because the broker is
                            // allowed to cut its last message short. The record layer decodes them, drops that
                            // partial trailing message and unwraps a compressed set into the messages it holds.
                            $messageSet = MessageSet::fromBuffer($responsePartition->messageSet ?? '', $checkCrcs);
                        } catch (KafkaException $exception) {
                            // A corrupt message only spoils its own partition, the others are still readable
                            $errors[$topic][$partitionId] = $exception;
                            continue;
                        }
                        $result[$topic][$partitionId] = new FetchedPartition(
                            new TopicPartition((string) $topic, (int) $partitionId),
                            $fetchOffset,
                            $responsePartition->errorCode,
                            $responsePartition->highWaterMarkOffset,
                            $messageSet,
                            $responsePartition->isSingleMessageTooLarge($fetchOffset)
                        );
                    }
                }

                return $result;
            },
            $timeout
        );
    }

    /**
     * Requests all offsets for the list of topic partitions
     *
     * This query will be made over the current cluster by checking the metadata for each topic partition
     *
     * @param array<string, array<int, int>> $topicPartitions Target times of each topic partition
     *
     * @return array<string, array<int, int>> Array in the form: [topic => [partition => offset]]
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     */
    public function fetchTopicPartitionOffsets(array $topicPartitions): array
    {
        return $this->clusterRequest(
            $topicPartitions,
            fn(array $nodeTopicRequest, int $correlationId): OffsetsRequest => new OffsetsRequest(
                $nodeTopicRequest,
                1,
                -1,
                $this->configuration[ConsumerConfig::CLIENT_ID],
                $correlationId
            ),
            OffsetsResponse::class,
            static function (array $result, OffsetsResponse $response, array &$errors): array {
                foreach ($response->topics as $topic => $topicResponse) {
                    /** @var OffsetsResponsePartition $partitionMetadata */
                    foreach ($topicResponse->partitions as $partitionId => $partitionMetadata) {
                        if ($partitionMetadata->errorCode !== KafkaException::NO_ERROR) {
                            $errors[$topic][$partitionId] = KafkaException::fromCode(
                                $partitionMetadata->errorCode,
                                ['topic' => $topic, 'partitionId' => $partitionId]
                            );
                            continue;
                        }
                        // v0 answers with a list of segment offsets, the newest one first
                        $result[$topic][$partitionId] = $partitionMetadata->offsets[0] ?? 0;
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Commits the offsets for topic partitions for the concrete consumer group
     *
     * The version of the request follows the `offsets.storage` option: version 1 stores the offsets in the
     * `__consumer_offsets` topic of the cluster and has to be sent to the coordinator of the group, version 0 stores
     * them in ZooKeeper and is answered by any broker. An offset may be given as a plain integer or as an
     * {@see OffsetAndMetadata}, which the broker keeps and hands back with the next OffsetFetch.
     *
     * @param Node                                                       $coordinatorNode       Current offset
     *        coordinator for $groupId
     * @param string                                                     $groupId               Name of the group
     * @param array<string, array<int, int|OffsetAndMetadata>>           $topicPartitionOffsets Offsets to commit
     *
     * @throws Common\Errors\OffsetMetadataTooLargeException
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     */
    public function commitGroupOffsets(Node $coordinatorNode, string $groupId, array $topicPartitionOffsets): void
    {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => $this->isOffsetStorageKafka()
                ? new OffsetCommitRequest(
                    $groupId,
                    OffsetCommitRequest::DEFAULT_GENERATION_ID,
                    OffsetCommitRequest::DEFAULT_MEMBER_NAME,
                    $topicPartitionOffsets,
                    $clientId,
                    $correlationId
                )
                : new OffsetCommitRequestV0($groupId, $topicPartitionOffsets, $clientId, $correlationId),
            OffsetCommitResponse::class,
            static function (OffsetCommitResponse $response) use ($groupId): void {
                foreach ($response->topics as $topic => $topicResponse) {
                    /** @var OffsetCommitResponsePartition $partition */
                    foreach ($topicResponse->partitions as $partitionId => $partition) {
                        if ($partition->errorCode !== KafkaException::NO_ERROR) {
                            throw KafkaException::fromCode(
                                $partition->errorCode,
                                ['groupId' => $groupId, 'topic' => $topic, 'partitionId' => $partitionId]
                            );
                        }
                    }
                }
            }
        );
    }

    /**
     * Fetches the offsets for topic partition for the concrete consumer group
     *
     * The version of the request follows the `offsets.storage` option, exactly like {@see self::commitGroupOffsets()}.
     * A topic-partition that has never been committed comes back with the offset -1: as the error code 0 from the
     * `__consumer_offsets` topic (v1), and as the error code 3 from ZooKeeper (v0).
     *
     * @param Node                          $coordinatorNode Current offset coordinator for $groupId
     * @param string                        $groupId         Name of the group
     * @param array<string, array<int, int>> $topicPartitions List of topic => partitions for fetching information
     *
     * @return array<string, array<int, int>> Committed offsets in the form [topic => [partition => offset]]
     *
     * Exception UnknownTopicOrPartition is ignored and silenced, offset -1 will be returned
     *
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\NotCoordinatorForGroupException
     */
    public function fetchGroupOffsets(Node $coordinatorNode, string $groupId, array $topicPartitions): array
    {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        return $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => $this->isOffsetStorageKafka()
                ? new OffsetFetchRequest($groupId, $topicPartitions, $clientId, $correlationId)
                : new OffsetFetchRequestV0($groupId, $topicPartitions, $clientId, $correlationId),
            OffsetFetchResponse::class,
            static function (OffsetFetchResponse $response) use ($groupId): array {
                $result = [];
                foreach ($response->topics as $topic => $topicResponse) {
                    /** @var OffsetFetchResponsePartition $partition */
                    foreach ($topicResponse->partitions as $partitionId => $partition) {
                        $isUnknownTopicPartition = $partition->errorCode === KafkaException::UNKNOWN_TOPIC_OR_PARTITION;
                        if ($partition->errorCode !== KafkaException::NO_ERROR && !$isUnknownTopicPartition) {
                            throw KafkaException::fromCode(
                                $partition->errorCode,
                                ['groupId' => $groupId, 'topic' => $topic, 'partitionId' => $partitionId]
                            );
                        }
                        $result[$topic][$partitionId] = $partition->offset;
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Discovers the coordinator node for the consumer group (ApiKey 10, called ConsumerMetadata in Kafka 0.8.2)
     *
     * The broker answers with error code 15 (ConsumerCoordinatorNotAvailable) while the internal __consumer_offsets
     * topic is still being created and with 14 (OffsetsLoadInProgress) while it reads the offsets of the group out
     * of it, so {@see CoordinatorLookup} retries both with `retry.backoff.ms` until `metadata.fetch.timeout.ms`.
     *
     * @param string $groupId Name of the group
     *
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     */
    public function getGroupCoordinator(string $groupId): Node
    {
        return new CoordinatorLookup($this->cluster, $this->configuration)->findCoordinator($groupId);
    }

    /**
     * Reduces the rich answer of each topic-partition to the records it carried
     *
     * @param array<string, array<int, FetchedPartition>> $topicPartitions
     *
     * @return array<string, array<int, list<Record>>>
     */
    private static function toRecordsByPartition(array $topicPartitions): array
    {
        $result = [];
        foreach ($topicPartitions as $topic => $partitions) {
            foreach ($partitions as $partitionId => $fetchedPartition) {
                $result[$topic][$partitionId] = $fetchedPartition->getRecords();
            }
        }

        return $result;
    }

    /**
     * Normalizes the messages of one topic-partition into the records that a message set is built from.
     *
     * The producer hands over {@see Record} instances; a plain string is accepted as well and becomes a record
     * without a key, which is what the callers of the older API pass.
     *
     * @param iterable<Record|string|\Stringable> $messages
     *
     * @return list<Record>
     */
    private static function toRecords(iterable $messages): array
    {
        $records = [];
        foreach ($messages as $message) {
            $records[] = $message instanceof Record ? $message : new Record((string) $message);
        }

        return $records;
    }

    /**
     * Checks whether the consumer offsets are stored in Kafka itself (v1) instead of ZooKeeper (v0)
     */
    private function isOffsetStorageKafka(): bool
    {
        $storage = $this->configuration[ClientConfig::OFFSETS_STORAGE] ?? ClientConfig::OFFSETS_STORAGE_KAFKA;

        return $storage === ClientConfig::OFFSETS_STORAGE_KAFKA;
    }

    /**
     * Sends one request to the coordinator of a group and hands its answer to the given reader.
     *
     * A dropped connection is the only failure that is worth another attempt here: every error code of the
     * OffsetCommit and OffsetFetch APIs is either final or has to be answered by looking the coordinator up again,
     * which is the business of the caller.
     *
     * @template T
     *
     * @param Node                                     $coordinatorNode Coordinator to talk to
     * @param Closure(int): AbstractRequest            $createRequest   Builds the request for a correlation id
     * @param class-string<AbstractResponse>   $responseClass   Class of the expected response
     * @param Closure(mixed): T                        $readResponse    Turns the response into the result
     *
     * @return T
     */
    private function coordinatorRequest(
        Node $coordinatorNode,
        Closure $createRequest,
        string $responseClass,
        Closure $readResponse
    ): mixed {
        $policy = RetryPolicy::fromConfiguration($this->configuration);

        return $policy->execute(function () use ($coordinatorNode, $createRequest, $responseClass, $readResponse) {
            $stream        = $coordinatorNode->getConnection($this->configuration);
            $correlationId = AbstractRequest::nextCorrelationId();

            $createRequest($correlationId)->writeTo($stream);

            try {
                $response = ResponseValidator::read(
                    $responseClass,
                    $stream,
                    $correlationId,
                    ['node' => $coordinatorNode->nodeId]
                );
            } catch (CorrelationIdMismatchException $exception) {
                ConnectionFactory::closeStream($stream);

                throw $exception;
            }

            return $readResponse($response);
        });
    }

    /**
     * Writes a request that the broker never answers to the leader of each topic-partition.
     *
     * This is the acks = 0 produce request: the record is considered sent as soon as it has been handed to the
     * socket, so there is no response to read and no error the broker could report.
     *
     * @param array<string, array<int, MessageSet>>       $topicPartitionMessageSets Message set of each partition
     * @param Closure(array, int): AbstractRequest        $createRequest             Builds the request of one node
     */
    private function fireAndForget(array $topicPartitionMessageSets, Closure $createRequest): void
    {
        $messageSetsByNode = [];
        foreach ($topicPartitionMessageSets as $topic => $partitionMessageSets) {
            foreach ($partitionMessageSets as $partition => $messageSet) {
                $leaderNode = $this->cluster->leaderFor((string) $topic, (int) $partition);

                $messageSetsByNode[$leaderNode->nodeId][$topic][$partition] = $messageSet;
            }
        }

        foreach ($messageSetsByNode as $nodeId => $nodeTopicPartitionMessageSets) {
            $stream = $this->connectionTo($nodeId);
            $createRequest($nodeTopicPartitionMessageSets, AbstractRequest::nextCorrelationId())->writeTo($stream);
        }
    }

    /**
     * Sends one request per partition leader and merges the answers, retrying what a metadata refresh can fix.
     *
     * @param array<string, array<int, mixed>>                  $topicPartitionsRequest Request data per partition
     * @param Closure(array, int): AbstractRequest              $nodeRequest            Builds the request of one node
     * @param class-string<AbstractResponse>            $responseClass          Class of the expected response
     * @param Closure(array, mixed, array): array               $responseAggregator     Merges one answer into the
     *        result and collects the error of each partition that failed
     * @param int|null                                          $timeout                How long to wait for the
     *        answers, `request.timeout.ms` by default
     *
     * @return array<string, array<int, mixed>>
     *
     * @throws TopicPartitionRequestException If at least one topic-partition could not be served
     */
    private function clusterRequest(
        array $topicPartitionsRequest,
        Closure $nodeRequest,
        string $responseClass,
        Closure $responseAggregator,
        ?int $timeout = null
    ): array {
        $policy          = RetryPolicy::fromConfiguration($this->configuration);
        $maxAttempts     = $policy->getMaxAttempts();
        $result          = [];
        $permanentErrors = [];
        $pending         = $topicPartitionsRequest;

        for ($attempt = 1;; $attempt++) {
            $errors = [];
            $result = self::mergeResult(
                $result,
                $this->dispatch($pending, $nodeRequest, $responseClass, $responseAggregator, $timeout, $errors)
            );

            $retriable = [];
            foreach ($errors as $topic => $partitionErrors) {
                foreach ($partitionErrors as $partitionId => $error) {
                    $canRetry = $attempt < $maxAttempts
                        && RetryPolicy::isRetriable($error)
                        && isset($pending[$topic][$partitionId]);
                    if ($canRetry) {
                        $retriable[$topic][$partitionId] = $pending[$topic][$partitionId];
                    } else {
                        $permanentErrors[$topic][$partitionId] = $error;
                    }
                }
            }
            if ($retriable === []) {
                break;
            }

            // The error codes 3, 5, 6 and a dropped connection all mean the same thing: the leader this client has
            // in its metadata is not the leader of that partition any more
            $this->reloadCluster();
            $policy->backoff();
            $pending = $retriable;
        }

        if ($permanentErrors !== []) {
            throw new TopicPartitionRequestException($result, $permanentErrors);
        }

        return $result;
    }

    /**
     * Performs one round of the fan-out: one request per leader, then the answers as they arrive
     *
     * @param array<string, array<int, mixed>>       $topicPartitionsRequest Request data per topic-partition
     * @param Closure(array, int): AbstractRequest   $nodeRequest            Builds the request of one node
     * @param class-string<AbstractResponse> $responseClass          Class of the expected response
     * @param Closure(array, mixed, array): array    $responseAggregator     Merges one answer into the result
     * @param int|null                               $timeout                How long to wait for the answers
     * @param array<string, array<int, Exception>>   $exceptions             Collects the error of each partition
     *
     * @return array<string, array<int, mixed>>
     */
    private function dispatch(
        array $topicPartitionsRequest,
        Closure $nodeRequest,
        string $responseClass,
        Closure $responseAggregator,
        ?int $timeout,
        array &$exceptions
    ): array {
        $requestByNode = [];
        foreach ($topicPartitionsRequest as $topic => $partitions) {
            foreach ($partitions as $partitionId => $partitionData) {
                try {
                    $leaderNode = $this->cluster->leaderFor((string) $topic, (int) $partitionId);

                    $requestByNode[$leaderNode->nodeId][$topic][$partitionId] = $partitionData;
                } catch (Exception $exception) {
                    $exceptions[$topic][$partitionId] = $exception;
                }
            }
        }

        /** @var array<int, Stream> $nodeStreams */
        $nodeStreams = [];
        /** @var array<int, resource> $nodeSockets */
        $nodeSockets = [];
        /** @var array<int, int> $correlationIds */
        $correlationIds = [];

        foreach ($requestByNode as $nodeId => $nodeTopicPartitions) {
            try {
                $correlationId = AbstractRequest::nextCorrelationId();
                $request       = $nodeRequest($nodeTopicPartitions, $correlationId);
                $stream        = $this->connectionTo($nodeId);
                if ($stream instanceof SocketStream) {
                    // Opened before the request is written, so that the answers of every leader can be awaited at
                    // once with stream_select() instead of one after another
                    $nodeSockets[$nodeId] = $stream->getStreamSocket();
                }
                $request->writeTo($stream);

                $nodeStreams[$nodeId]    = $stream;
                $correlationIds[$nodeId] = $correlationId;
            } catch (Exception $exception) {
                unset($nodeSockets[$nodeId]);
                self::assignNodeError($exceptions, $nodeTopicPartitions, $exception);
            }
        }

        $responses = [];
        $readNode  = function (int $nodeId) use (
            $nodeStreams,
            $correlationIds,
            $responseClass,
            $requestByNode,
            &$responses,
            &$exceptions
        ): void {
            $stream = $nodeStreams[$nodeId];
            try {
                $responses[$nodeId] = ResponseValidator::read(
                    $responseClass,
                    $stream,
                    $correlationIds[$nodeId],
                    ['node' => $nodeId]
                );
            } catch (CorrelationIdMismatchException $exception) {
                // The stream position of a desynchronized connection is unknown, it must not be used again
                ConnectionFactory::closeStream($stream);
                self::assignNodeError($exceptions, $requestByNode[$nodeId], $exception);
            } catch (Exception $exception) {
                self::assignNodeError($exceptions, $requestByNode[$nodeId], $exception);
            }
        };

        // Streams that stream_select() can not watch - the in-memory doubles of the unit tests - are read directly
        foreach (array_keys($nodeStreams) as $nodeId) {
            if (!isset($nodeSockets[$nodeId])) {
                $readNode($nodeId);
            }
        }

        $timeout ??= (int) ($this->configuration[ClientConfig::REQUEST_TIMEOUT_MS] ?? self::DEFAULT_REQUEST_TIMEOUT_MS);
        $incompleteReads = $nodeSockets;
        $finishTime      = microtime(true) + 2 * ($timeout / 1000);
        while ($incompleteReads !== []) {
            $readSelect  = $incompleteReads;
            $writeSelect = $exceptSelect = null;
            $readyCount  = @stream_select(
                $readSelect,
                $writeSelect,
                $exceptSelect,
                intdiv($timeout, 1000),
                ($timeout % 1000) * 1000
            );
            if ($readyCount > 0) {
                foreach ($readSelect as $resourceToRead) {
                    $nodeId = array_search($resourceToRead, $nodeSockets, true);
                    if ($nodeId === false) {
                        continue;
                    }
                    unset($incompleteReads[$nodeId]);
                    $readNode($nodeId);
                }
            }
            if ($incompleteReads === [] || microtime(true) >= $finishTime) {
                break;
            }
        }

        foreach (array_keys($incompleteReads) as $nodeId) {
            self::assignNodeError(
                $exceptions,
                $requestByNode[$nodeId],
                new NetworkException(
                    ['error' => 'Timeout while waiting for the response', 'node' => $nodeId, 'timeoutMs' => $timeout]
                )
            );
        }

        $result = [];
        foreach ($responses as $response) {
            $result = $responseAggregator($result, $response, $exceptions);
        }

        return $result;
    }

    /**
     * Returns the connection to the broker with the given node id
     *
     * @throws UnknownErrorException If the cluster does not know that node any more
     */
    private function connectionTo(int $nodeId): Stream
    {
        $node = $this->cluster->nodeById($nodeId);
        if ($node === null) {
            throw new UnknownErrorException(['error' => 'Node was not found in the cluster', 'nodeId' => $nodeId]);
        }

        return $node->getConnection($this->configuration);
    }

    /**
     * Refreshes the cluster metadata, keeping the current one if the cluster can not be reached.
     *
     * The error of the request itself is the one worth reporting: a failing refresh only means that the next
     * attempt will use the very same metadata.
     */
    private function reloadCluster(): void
    {
        try {
            $this->cluster->reload();
        } catch (KafkaException) {
            // Keep the metadata that is already known
        }
    }

    /**
     * Records the same error for every topic-partition that was sent to one broker
     *
     * @param array<string, array<int, Exception>> $exceptions          Collected errors
     * @param array<string, array<int, mixed>>     $nodeTopicPartitions Partitions that were sent to that broker
     */
    private static function assignNodeError(array &$exceptions, array $nodeTopicPartitions, Exception $error): void
    {
        foreach ($nodeTopicPartitions as $topic => $partitions) {
            foreach (array_keys($partitions) as $partitionId) {
                $exceptions[$topic][$partitionId] = $error;
            }
        }
    }

    /**
     * Merges the result of one attempt into the result of the previous ones
     *
     * @param array<string, array<int, mixed>> $result   Result collected so far
     * @param array<string, array<int, mixed>> $addition Result of the last attempt
     *
     * @return array<string, array<int, mixed>>
     */
    private static function mergeResult(array $result, array $addition): array
    {
        foreach ($addition as $topic => $partitions) {
            foreach ($partitions as $partitionId => $value) {
                $result[$topic][$partitionId] = $value;
            }
        }

        return $result;
    }
}
