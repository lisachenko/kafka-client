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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig as ConsumerConfig;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Consumer\OffsetAndTimestamp;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\ProducerConfig as ProducerConfig;
use Protocol\Kafka\Protocol\Data\DeleteRecordsResponsePartition;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequest;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponse;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponse;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV2;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;

/**
 * Low-level client for the Kafka 0.11.0.3 protocol.
 *
 * Every api is sent with the highest version a 0.11.0.3 broker serves: Produce v3, which carries a record batch of
 * the message format v2 and the transactional id of its producer and whose answer reports the `LogAppendTime` and
 * the `LogStartOffset` of every partition, Fetch v5, which asks for the log as it lies, bounds the whole answer
 * with `fetch.max.bytes` and states the isolation level of the consumer, OffsetCommit v3 with its `retention_time`,
 * and OffsetCommit v0 when the offsets are stored in ZooKeeper. The apis that KIP-124 raised go out with the
 * version whose answer carries a `throttle_time_ms` - Metadata v4, Offsets v2, OffsetFetch v3, GroupCoordinator v1
 * and the group membership apis one version up. The lower version classes of those apis stay usable directly, for a
 * client that has to talk to an older broker - and `message.format.version` lowers the Produce request to v2 by
 * itself, because a message set of the formats v0 and v1 has no place in a version 3 request.
 *
 * Every request that addresses topic-partitions is split by their current leader and sent to all of those brokers
 * at once; the answers are collected with `stream_select()` as they arrive. A topic-partition whose leader answered
 * with a retriable error - 3 UnknownTopicOrPartition, 5 LeaderNotAvailable, 6 NotLeaderForPartition - or whose
 * connection dropped is sent again after the cluster metadata has been refreshed, up to `retries` times. What is
 * still broken afterwards is reported as a {@see TopicPartitionRequestException} that carries both the partial
 * result of the partitions that did succeed and the error of each partition that did not.
 *
 * @see docs/protocol/0.11.0.md
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
     * Asks one broker which api keys and versions it serves (ApiKey 18, Kafka 0.10.0 and later)
     *
     * This is the answer to "what does the broker on the other side speak": the 0.8 and 0.9 lines of this client had
     * to probe it by sending a request of every key and version, because the api did not exist yet. A 0.11.0.3
     * broker reports the 34 keys 0 to 33 with the version ranges of the api-key table of the protocol document, and
     * answers before any authentication has happened on a SASL listener.
     *
     * The request goes out as **version 1**, the version Kafka 0.11 added: its frame is the one of version 0 - the
     * header and nothing else - and the answer gains a trailing `throttleTimeMs`, which is 0 without a
     * `request_percentage` quota.
     *
     * The client itself does **not** negotiate with the answer - like the `0.9.x` line it sends the fixed versions
     * that a broker of its own Kafka release serves - so this is an api for callers that want to know what they are
     * talking to, and the material a later line can build a negotiation on.
     *
     * The request is the one frame of the protocol whose *unsupported version* is answered instead of costing the
     * connection: a broker that does not know the version answers the error code 35 (UnsupportedVersion) with an
     * empty api array. That answer always arrives in the **version 0** layout, without the throttle time, so a peer
     * older than Kafka 0.11 has to be asked with a {@see \Protocol\Kafka\Protocol\Request\ApiVersionsRequestV0} and
     * read with an {@see \Protocol\Kafka\Protocol\Request\ApiVersionsResponseV0}; this line speaks to a 0.11.0.3
     * broker, which serves both versions.
     *
     * @param Node $node Broker to ask; every broker of a cluster answers for itself
     *
     * @throws NetworkException If the connection to the broker dropped
     */
    public function apiVersions(Node $node): ApiVersionsResponse
    {
        return $this->coordinatorRequest(
            $node,
            fn(int $correlationId): ApiVersionsRequest => new ApiVersionsRequest(
                $this->configuration[ClientConfig::CLIENT_ID],
                $correlationId
            ),
            ApiVersionsResponse::class,
            static fn(ApiVersionsResponse $response): ApiVersionsResponse => $response
        );
    }

    /**
     * Produce messages to the specific topic partition
     *
     * The request goes out as **Produce v3** for the message format v2 (`message.format.version=0.11.0`, the
     * default) and as Produce v2 for the legacy message sets of the formats v0 and v1, which a version 3 request
     * has no place for. Every accepted partition carries two values the broker reported next to its base offset:
     * the `logAppendTime` it stamped on the whole batch, which is -1 unless the topic is configured with
     * `message.timestamp.type=LogAppendTime`, and the `throttleTimeMs` of the answer it arrived in, which is 0
     * without a `producer_byte_rate` quota. Version 3 added no field to the answer at all - `PRODUCE_RESPONSE_V3`
     * is `PRODUCE_RESPONSE_V2` in `Protocol.java` @ 0.11.0.3 - so the two classes read the same frame.
     *
     * @param array<string, array<int, iterable<Record|string|\Stringable>>> $topicPartitionMessages Messages for
     *        each topic and partition
     *
     * @return array<string, array<int, ProduceResponsePartition>> Accepted partitions in the form
     *         [topic => [partition => ProduceResponsePartition]], empty for a fire-and-forget request (acks = 0),
     *         which the broker never answers
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     * @throws InvalidConfigurationException  For a `compression.type` or a `message.format.version` that this client
     *         can not write
     */
    public function produce(array $topicPartitionMessages): array
    {
        return $this->produceRecords($topicPartitionMessages);
    }

    /**
     * Appends the records of a batch, with the producer state that an idempotent or transactional producer holds.
     *
     * This is the single place that turns records into the record sets of a Produce request, so that the
     * bookkeeping of KIP-98 only has to fill in its four values instead of rebuilding the request: the producer id
     * and the epoch that `InitProducerId` handed out, the sequence number each topic-partition continues at, and
     * the transactional id the batch is written under. Their defaults - -1, -1, no sequence and `null` - are
     * exactly what a plain producer sends, and a batch that carries none of them is not deduplicated by the broker.
     *
     * The sequence numbers are *per topic-partition*, because that is the space the broker deduplicates in
     * (`ProducerStateManager` @ 0.11.0.3 keeps one sequence per producer **and** partition), so one call of this
     * method needs one of them per partition it writes to.
     *
     * @param array<string, array<int, iterable<Record|string|\Stringable>>> $topicPartitionMessages Messages for
     *        each topic and partition
     * @param int                            $producerId      Producer id of the batch, -1 without one
     * @param int                            $producerEpoch   Epoch of that producer, -1 without one
     * @param array<string, array<int, int>> $baseSequences   Sequence number the batch of a topic-partition starts
     *                                                        at, as topic => partition => sequence; a partition
     *                                                        that is not listed is written without one
     * @param string|null                    $transactionalId Transactional id of the producer, `null` outside of a
     *                                                        transaction
     *
     * @return array<string, array<int, ProduceResponsePartition>> Accepted partitions in the form
     *         [topic => [partition => ProduceResponsePartition]], empty for a fire-and-forget request (acks = 0)
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     * @throws InvalidConfigurationException  For a `compression.type` or a `message.format.version` that this client
     *         can not write, and for producer state that the configured message format has no place for
     */
    protected function produceRecords(
        array $topicPartitionMessages,
        int $producerId = RecordBatch::NO_PRODUCER_ID,
        int $producerEpoch = RecordBatch::NO_PRODUCER_EPOCH,
        array $baseSequences = [],
        ?string $transactionalId = null
    ): array {
        $requiredAcks = (int) $this->configuration[ProducerConfig::ACKS];

        // `compression.type` compresses a whole batch at once, so it is applied per topic-partition, not per record
        $compressionCodec = ProducerConfig::compressionCodec(
            $this->configuration[ProducerConfig::COMPRESSION_TYPE] ?? ProducerConfig::COMPRESSION_TYPE_NONE
        );
        // `message.format.version` decides which of the two message formats of this line the batch is written in
        $messageFormatMagic = ProducerConfig::messageFormatMagic(
            $this->configuration[ProducerConfig::MESSAGE_FORMAT_VERSION] ?? ProducerConfig::MESSAGE_FORMAT_VERSION_0_11_0
        );

        if ($messageFormatMagic < RecordBatch::MAGIC && $transactionalId !== null) {
            throw new InvalidConfigurationException(
                'A transactional producer needs the message format 0.11.0: the transactional id travels in a '
                . 'Produce v3 request, which only carries a record batch of the message format v2'
            );
        }

        // The wire format carries one opaque record set per topic-partition, see docs/protocol/0.11.0.md
        $topicPartitionRecordSets = [];
        foreach ($topicPartitionMessages as $topic => $partitionMessages) {
            foreach ($partitionMessages as $partition => $messages) {
                $topicPartitionRecordSets[$topic][$partition] = self::toRecordSet(
                    self::toRecords($messages),
                    $compressionCodec,
                    $messageFormatMagic,
                    $producerId,
                    $producerEpoch,
                    (int) ($baseSequences[$topic][$partition] ?? RecordBatch::NO_SEQUENCE),
                    $transactionalId !== null
                );
            }
        }

        // A message set of the formats v0 and v1 can only be sent with a version below 3, which is also the highest
        // version that has no place for a transactional id
        $requestClass  = $messageFormatMagic >= RecordBatch::MAGIC ? ProduceRequest::class : ProduceRequestV2::class;
        $createRequest = fn(array $nodeTopicPartitionRecordSets, int $correlationId): ProduceRequest
            => new $requestClass(
                $nodeTopicPartitionRecordSets,
                $requiredAcks,
                $this->configuration[ProducerConfig::TIMEOUT_MS],
                $this->configuration[ProducerConfig::CLIENT_ID],
                $correlationId,
                $transactionalId
            );
        $responseClass = $messageFormatMagic >= RecordBatch::MAGIC ? ProduceResponse::class : ProduceResponseV2::class;

        // acks = 0 is the only request of the protocol that the broker does not answer, so nothing may be read back
        // from those connections, see ProduceRequest::expectsResponse()
        if ($requiredAcks === ProduceRequest::ACKS_NONE) {
            $this->fireAndForget($topicPartitionRecordSets, $createRequest);

            return [];
        }

        return $this->clusterRequest(
            $topicPartitionRecordSets,
            $createRequest,
            $responseClass,
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
                        // Version 1 reports one ThrottleTime for the whole answer, and a batch is split by
                        // partition leaders, so the value of this answer is carried onto every partition of it
                        $partitionInfo->throttleTimeMs = $response->throttleTime;

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
     * how far behind the end of the log it is, and up to version 2 of the api a message that is larger than
     * `max.partition.fetch.bytes` makes the broker answer without an error and without a single complete message,
     * which would turn a naive fetch loop into an endless one, see
     * {@see FetchedPartition::isSingleMessageTooLarge()}.
     *
     * The request goes out as **Fetch v5**, which means four things:
     *
     * * the record sets come back in the format the log holds them in - a broker converts them down to message
     *   format v1 for a request below version 4 and to format v0 below version 2 - so the records carry their
     *   headers, their timestamps and the producer state of the message format v2;
     * * the whole answer is bounded by `fetch.max.bytes` on top of the per-partition `max.partition.fetch.bytes`.
     *   The broker fills the partitions **in the order of `$topicPartitionOffsets`** and stops once that budget is
     *   used up, so the partitions at the end of a large fetch come back empty; a caller that fetches more than one
     *   partition has to rotate their order between calls, as {@see \Protocol\Kafka\Consumer\KafkaConsumer} does;
     * * the first non-empty partition of the answer ignores both limits and carries at least one complete message,
     *   so a partition can no longer be stuck on a message that is too large and this client never reports one;
     * * every partition of the answer reports its `lastStableOffset`, its `logStartOffset` and the transactions
     *   that were aborted in the range it covers, and the request states the `isolation.level` of the consumer -
     *   `read_uncommitted` unless it is configured otherwise, which is what the broker answers a -1 last stable
     *   offset and a `null` aborted-transactions array to.
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
        $checkCrcs      = (bool) ($this->configuration[ConsumerConfig::CHECK_CRCS] ?? true);
        $isolationLevel = $this->isolationLevel();

        return $this->clusterRequest(
            $topicPartitionOffsets,
            fn(array $nodeTopicRequest, int $correlationId): FetchRequest => new FetchRequest(
                $nodeTopicRequest,
                $timeout,
                $this->configuration[ConsumerConfig::FETCH_MIN_BYTES],
                $this->configuration[ConsumerConfig::MAX_PARTITION_FETCH_BYTES],
                -1,
                $this->configuration[ConsumerConfig::CLIENT_ID],
                $correlationId,
                (int) ($this->configuration[ConsumerConfig::FETCH_MAX_BYTES] ?? FetchRequest::DEFAULT_MAX_BYTES),
                $isolationLevel
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
                            // The schema engine hands over the raw bytes of the record set, because the broker is
                            // allowed to cut its last batch short. The record layer looks at the message format of
                            // every batch, drops that partial trailing one, unwraps a compressed batch into the
                            // records it holds and keeps the control markers of a transaction to itself.
                            $records = MemoryRecords::fromBuffer($responsePartition->messageSet ?? '', $checkCrcs);
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
                            $records,
                            // From version 3 on the broker guarantees that the first non-empty partition of an
                            // answer holds a complete message, and an empty partition simply means that the
                            // `fetch.max.bytes` of the answer were used up by the ones in front of it
                            FetchRequest::VERSION < 3 && $responsePartition->isSingleMessageTooLarge($fetchOffset),
                            $response->throttleTimeMs,
                            $responsePartition->lastStableOffset,
                            $responsePartition->logStartOffset,
                            $responsePartition->abortedTransactions
                        );
                    }
                }

                return $result;
            },
            $timeout
        );
    }

    /**
     * Requests one offset for each of the given topic partitions
     *
     * This query will be made over the current cluster by checking the metadata for each topic partition; the
     * Offsets api is served by the leader of a partition alone.
     *
     * A target time is {@see OffsetsRequest::LATEST} for the log end offset - the offset the next produced message
     * will get - {@see OffsetsRequest::EARLIEST} for the first offset that is still on disk, or a timestamp in
     * milliseconds, which version 1 of the api (Kafka 0.10.1) answers with the offset of the first message whose own
     * timestamp is at or after it. A timestamp that no message of a partition matches is not an error: the offset of
     * that partition is then {@see OffsetsResponsePartition::UNKNOWN_OFFSET}, i.e. -1. Use
     * {@see self::fetchTopicPartitionOffsetsForTimes()} to receive the timestamp of the message that was found
     * together with its offset.
     *
     * @param array<string, array<int, int>> $topicPartitionTimestamps Target times of each topic partition
     *
     * @return array<string, array<int, int>> Array in the form: [topic => [partition => offset]]
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     */
    public function fetchTopicPartitionOffsets(array $topicPartitionTimestamps): array
    {
        $found = $this->fetchTopicPartitionOffsetsForTimes($topicPartitionTimestamps);

        $result = [];
        foreach ($found as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partitionId => $offsetAndTimestamp) {
                $result[$topic][$partitionId] = $offsetAndTimestamp?->offset ?? OffsetsResponsePartition::UNKNOWN_OFFSET;
            }
        }

        return $result;
    }

    /**
     * Looks the offsets of the given topic partitions up and reports the timestamp of every message that was found
     *
     * The timestamp-based version 1 of the Offsets api answers each partition with one offset and the timestamp of
     * the message it points at. A partition whose log holds no message at or after the target time - and every
     * partition of an empty log - is answered with the error code 0 and the offset -1, which arrives here as `null`.
     * {@see OffsetsRequest::LATEST} and {@see OffsetsRequest::EARLIEST} always find an offset, and the broker
     * answers them with the timestamp {@see OffsetsResponsePartition::UNKNOWN_TIMESTAMP}, because it does not read
     * the message the offset points at.
     *
     * @param array<string, array<int, int>> $topicPartitionTimestamps Target times of each topic partition
     *
     * @return array<string, array<int, OffsetAndTimestamp|null>> [topic => [partition => offset and timestamp]]
     *
     * @throws TopicPartitionRequestException If a partition was answered with an error code - which is how the
     *         `UnsupportedForMessageFormatException` of a topic whose `message.format.version` is older than 0.10.0
     *         arrives, since such a log has no message timestamps to search
     */
    public function fetchTopicPartitionOffsetsForTimes(array $topicPartitionTimestamps): array
    {
        return $this->clusterRequest(
            $topicPartitionTimestamps,
            fn(array $nodeTopicRequest, int $correlationId): OffsetsRequest => new OffsetsRequest(
                $nodeTopicRequest,
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
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
                        // "No message matches that timestamp" is answered with the offset -1 and no error at all
                        $result[$topic][$partitionId] = $partitionMetadata->offset === OffsetsResponsePartition::UNKNOWN_OFFSET
                            ? null
                            : new OffsetAndTimestamp($partitionMetadata->offset, $partitionMetadata->timestamp);
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Commits the offsets for topic partitions for the concrete consumer group
     *
     * The version of the request follows the `offsets.storage` option: version 3 stores the offsets in the
     * `__consumer_offsets` topic of the cluster and has to be sent to the coordinator of the group, version 0 stores
     * them in ZooKeeper and is answered by any broker. An offset may be given as a plain integer or as an
     * {@see OffsetAndMetadata}, which the broker keeps and hands back with the next OffsetFetch.
     *
     * `$retentionTimeMs` is the `retention_time` field of the v2 request, which v3 sends unchanged: with
     * {@see OffsetCommitRequest::DEFAULT_RETENTION_TIME} the broker keeps the offsets for `offsets.retention.minutes`
     * counted from its receive time, any other value replaces that retention for this commit. The ZooKeeper version
     * has no such field and ignores it. A client that is not a member of a group commits with
     * {@see OffsetCommitRequest::DEFAULT_GENERATION_ID} and {@see OffsetCommitRequest::DEFAULT_MEMBER_NAME}; a member
     * of a group has to pass the generation and the member id the coordinator assigned to it, otherwise the
     * coordinator answers with 22 (IllegalGeneration) or 25 (UnknownMemberId).
     *
     * @param Node                                             $coordinatorNode       Current offset coordinator for
     *        $groupId
     * @param string                                           $groupId               Name of the group
     * @param string                                           $memberId              Member id inside the group
     * @param int                                              $generationId          Generation of the group
     * @param array<string, array<int, int|OffsetAndMetadata>> $topicPartitionOffsets Offsets to commit
     * @param int                                              $retentionTimeMs       How long the broker keeps them
     *
     * @throws Common\Errors\OffsetMetadataTooLargeException
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     */
    public function commitGroupOffsets(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        int $generationId,
        array $topicPartitionOffsets,
        int $retentionTimeMs
    ): void {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => $this->isOffsetStorageKafka()
                ? new OffsetCommitRequest(
                    $groupId,
                    $generationId,
                    $memberId,
                    $retentionTimeMs,
                    $topicPartitionOffsets,
                    $clientId,
                    $correlationId
                )
                : new OffsetCommitRequestV0($groupId, $topicPartitionOffsets, $clientId, $correlationId),
            // The version 3 answer opens with the throttle time of KIP-124, which a version 0 one does not have
            $this->isOffsetStorageKafka() ? OffsetCommitResponse::class : OffsetCommitResponseV0::class,
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
     * The version of the request follows the `offsets.storage` option, exactly like {@see self::commitGroupOffsets()}
     * - `kafka` reads them out of `__consumer_offsets` with the version 2 of the api, `zookeeper` with the version 0.
     * A topic-partition that has never been committed comes back with the offset -1: as the error code 0 from the
     * `__consumer_offsets` topic (v1 and v2), and as the error code 3 from ZooKeeper (v0).
     *
     * `$topicPartitions` of **null** asks the coordinator for every topic-partition the group has a committed offset
     * for, which the nullable topic array of the version 2 (Kafka 0.10.2) makes possible; an **empty** array names no
     * topic at all and is answered with an empty result. Reading all topics needs the Kafka storage: version 0 has no
     * nullable array and refuses it with an {@see Common\Errors\UnsupportedVersionException}.
     *
     * @param Node                                $coordinatorNode Current offset coordinator for $groupId
     * @param string                              $groupId         Name of the group
     * @param array<string, array<int, int>>|null $topicPartitions List of topic => partitions for fetching
     *        information, or null for every topic of the group
     *
     * @return array<string, array<int, int>> Committed offsets in the form [topic => [partition => offset]]
     *
     * Exception UnknownTopicOrPartition is ignored and silenced, offset -1 will be returned
     *
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     * @throws Common\Errors\GroupAuthorizationFailedException
     */
    public function fetchGroupOffsets(Node $coordinatorNode, string $groupId, ?array $topicPartitions): array
    {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        return $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => $this->isOffsetStorageKafka()
                ? new OffsetFetchRequest($groupId, $topicPartitions, $clientId, $correlationId)
                : new OffsetFetchRequestV0($groupId, $topicPartitions, $clientId, $correlationId),
            $this->isOffsetStorageKafka() ? OffsetFetchResponse::class : OffsetFetchResponseV0::class,
            static function (OffsetFetchResponse $response) use ($groupId): array {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    // Version 2 reports what is wrong with the group itself here, and answers no topic at all
                    throw KafkaException::fromCode($response->errorCode, ['groupId' => $groupId]);
                }

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
     * Joins the group with the specified protocol and member information (ApiKey 11, Kafka 0.9)
     *
     * A client that has no member id yet passes {@see JoinGroupRequest::DEFAULT_MEMBER_ID} and receives the id the
     * coordinator assigned to it; a member that rejoins has to pass the id of the previous generation. The answer
     * names the generation, the protocol the coordinator picked out of `$groupProtocols` and the leader of the
     * group - the member whose id equals the `leaderId` of the answer is the one that computes the assignment and
     * publishes it with {@see self::syncGroup()}; only that member receives the `members` array.
     *
     * **The coordinator holds this request until the rebalance is over**, i.e. until every known member of the
     * group has rejoined or has run out of time. How much time each of them gets is the `rebalance_timeout` of the
     * version 1 request (Kafka 0.10.1): the coordinator waits the **largest** rebalance timeout of the members of
     * the group, not their session timeout. `request.timeout.ms` - the read timeout of the socket - therefore has to
     * be larger than both {@see ConsumerConfig::SESSION_TIMEOUT_MS} and {@see ConsumerConfig::MAX_POLL_INTERVAL_MS},
     * which are where the two timeouts of the request come from.
     *
     * @param Node                  $coordinatorNode   Current group coordinator for $groupId
     * @param string                $groupId           Name of the group
     * @param string                $memberId          Name of the group member, empty when it has none yet
     * @param string                $protocolType      Type of protocol to use for joining, e.g. `consumer`
     * @param array<string, string> $groupProtocols    Metadata of every supported protocol, by protocol name; opaque
     *        bytes to this api - a `consumer` member sends its `Subscription` here
     * @param int|null              $rebalanceTimeoutMs How long the coordinator may wait for this member to rejoin a
     *        rebalance, null for the configured `max.poll.interval.ms`
     *
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     * @throws Common\Errors\InconsistentGroupProtocolException
     * @throws Common\Errors\UnknownMemberIdException
     * @throws Common\Errors\InvalidSessionTimeoutException
     * @throws Common\Errors\InvalidGroupIdException
     * @throws Common\Errors\GroupAuthorizationFailedException
     */
    public function joinGroup(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        string $protocolType,
        array $groupProtocols,
        ?int $rebalanceTimeoutMs = null
    ): JoinGroupResponse {
        $clientId         = (string) $this->configuration[ConsumerConfig::CLIENT_ID];
        $sessionTimeout   = (int) $this->configuration[ConsumerConfig::SESSION_TIMEOUT_MS];
        $rebalanceTimeout = $rebalanceTimeoutMs
            ?? (int) ($this->configuration[ConsumerConfig::MAX_POLL_INTERVAL_MS]
                ?? ConsumerConfig::DEFAULT_MAX_POLL_INTERVAL_MS);

        return $this->groupRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => new JoinGroupRequest(
                $groupId,
                $sessionTimeout,
                $rebalanceTimeout,
                $memberId,
                $protocolType,
                $groupProtocols,
                $clientId,
                $correlationId
            ),
            JoinGroupResponse::class,
            static function (JoinGroupResponse $response) use ($groupId, $memberId, $protocolType): JoinGroupResponse {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['groupId' => $groupId, 'memberId' => $memberId, 'protocolType' => $protocolType]
                    );
                }

                return $response;
            }
        );
    }

    /**
     * Synchronizes a group member with the group and returns its assignment (ApiKey 14, Kafka 0.9)
     *
     * Every member sends this request right after it joined, but only the leader of the generation passes an
     * assignment for each member; every other member passes an empty array and receives its own share in the
     * answer, which the coordinator holds back until the leader has sent the assignment.
     *
     * @param Node                  $coordinatorNode  Current group coordinator for $groupId
     * @param string                $groupId          Name of the group
     * @param string                $memberId         Name of the group member
     * @param int                   $generationId     Current generation of the group
     * @param array<string, string> $groupAssignments Assignment of every member, by member id, sent by the leader
     *        only; opaque bytes to this api - a `consumer` leader sends a `MemberAssignment` per member
     *
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     * @throws Common\Errors\IllegalGenerationException
     * @throws Common\Errors\UnknownMemberIdException
     * @throws Common\Errors\RebalanceInProgressException
     * @throws Common\Errors\GroupAuthorizationFailedException
     */
    public function syncGroup(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        int $generationId,
        array $groupAssignments = []
    ): SyncGroupResponse {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        return $this->groupRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => new SyncGroupRequest(
                $groupId,
                $generationId,
                $memberId,
                $groupAssignments,
                $clientId,
                $correlationId
            ),
            SyncGroupResponse::class,
            static function (SyncGroupResponse $response) use ($groupId, $memberId, $generationId): SyncGroupResponse {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['groupId' => $groupId, 'memberId' => $memberId, 'generationId' => $generationId]
                    );
                }

                return $response;
            }
        );
    }

    /**
     * Sends a heartbeat for the current member of the group (ApiKey 12, Kafka 0.9)
     *
     * A successful heartbeat resets the session timeout of the member. The error code of the answer is how the
     * coordinator tells the member what happened to the group meanwhile, and all three cases are reported as their
     * exception: 27 RebalanceInProgress - rejoin, 25 UnknownMemberId - the member was dropped and has to join
     * again without a member id, 22 IllegalGeneration - the generation of the member is over.
     *
     * @param Node   $coordinatorNode Current group coordinator for $groupId
     * @param string $groupId         Name of the group
     * @param string $memberId        Name of the group member
     * @param int    $generationId    Current generation of the group
     *
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     * @throws Common\Errors\IllegalGenerationException
     * @throws Common\Errors\UnknownMemberIdException
     * @throws Common\Errors\RebalanceInProgressException
     * @throws Common\Errors\GroupAuthorizationFailedException
     */
    public function heartbeat(Node $coordinatorNode, string $groupId, string $memberId, int $generationId): void
    {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        $this->groupRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => new HeartbeatRequest(
                $groupId,
                $generationId,
                $memberId,
                $clientId,
                $correlationId
            ),
            HeartbeatResponse::class,
            static function (HeartbeatResponse $response) use ($groupId, $memberId, $generationId): void {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['groupId' => $groupId, 'memberId' => $memberId, 'generationId' => $generationId]
                    );
                }
            }
        );
    }

    /**
     * Removes the group member from its group (ApiKey 13, Kafka 0.9)
     *
     * The group rebalances right away instead of waiting for the session timeout of the member to expire, so this
     * is what a consumer sends when it shuts down in an orderly way.
     *
     * @param Node   $coordinatorNode Current group coordinator for $groupId
     * @param string $groupId         Name of the group
     * @param string $memberId        Name of the group member
     *
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     * @throws Common\Errors\UnknownMemberIdException
     * @throws Common\Errors\GroupAuthorizationFailedException
     */
    public function leaveGroup(Node $coordinatorNode, string $groupId, string $memberId): void
    {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        $this->groupRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => new LeaveGroupRequest(
                $groupId,
                $memberId,
                $clientId,
                $correlationId
            ),
            LeaveGroupResponse::class,
            static function (LeaveGroupResponse $response) use ($groupId, $memberId): void {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['groupId' => $groupId, 'memberId' => $memberId]
                    );
                }
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
        return new CoordinatorLookup($this->cluster, $this->configuration)->findCoordinator(
            $groupId,
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP
        );
    }

    /**
     * Discovers the coordinator node of a transactional id (ApiKey 10 v1, `coordinator_type = 1`, Kafka 0.11)
     *
     * The transaction coordinator is the broker that owns the partition of the internal `__transaction_state` topic
     * that the transactional id hashes to; it is the one that serves InitProducerId, AddPartitionsToTxn,
     * AddOffsetsToTxn, EndTxn and TxnOffsetCommit for that producer, and it is looked up with the very same api as
     * a group coordinator, only with {@see GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION}.
     *
     * **The first lookup of any transactional id creates `__transaction_state`**, exactly as the first group lookup
     * creates `__consumer_offsets`, and is therefore answered with the error code 15
     * (GroupCoordinatorNotAvailable) while that topic is being created; {@see CoordinatorLookup} retries it. The id
     * itself is not created or registered by the lookup - the broker only hashes it onto a partition of that topic -
     * so asking for an id that was never used is a legal question with a normal answer.
     *
     * @param string $transactionalId The `transactional.id` of the producer
     *
     * @throws Common\Errors\GroupCoordinatorNotAvailableException If the coordinator did not become available
     * @throws Common\Errors\InvalidRequestException If the broker refuses the coordinator type
     */
    public function getTransactionCoordinator(string $transactionalId): Node
    {
        return new CoordinatorLookup($this->cluster, $this->configuration)->findCoordinator(
            $transactionalId,
            GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION
        );
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
     * Builds the byte region that one topic-partition of a Produce request carries, in the configured format.
     *
     * The message format v2 is a {@see RecordBatch} - one batch per topic-partition, compressed as a whole, with
     * the producer state and the record headers in it - and the formats v0 and v1 are a {@see MessageSet}, which
     * has no place for either and silently drops the headers of a record. {@see MemoryRecords} wraps both, so that
     * the request does not have to know which of them it is sending.
     *
     * @param list<Record> $records          Records of this topic-partition, with their `CreateTime` timestamps
     * @param int          $compressionCodec Codec that compresses the whole batch, {@see \Protocol\Kafka\Common\Record\CompressionCodec::NONE}
     *                                       for an uncompressed one
     * @param int          $messageFormatMagic Magic byte of the message format to write
     * @param int          $producerId       Producer id of an idempotent producer, -1 without one
     * @param int          $producerEpoch    Epoch of that producer, -1 without one
     * @param int          $baseSequence     Sequence number of the first record, -1 without a producer id
     * @param bool         $isTransactional  Whether the batch belongs to a transaction
     */
    private static function toRecordSet(
        array $records,
        int $compressionCodec,
        int $messageFormatMagic,
        int $producerId = RecordBatch::NO_PRODUCER_ID,
        int $producerEpoch = RecordBatch::NO_PRODUCER_EPOCH,
        int $baseSequence = RecordBatch::NO_SEQUENCE,
        bool $isTransactional = false
    ): MemoryRecords {
        if ($messageFormatMagic >= RecordBatch::MAGIC) {
            return MemoryRecords::fromRecordBatch(RecordBatch::fromRecords(
                $records,
                $compressionCodec,
                0,
                $producerId,
                $producerEpoch,
                $baseSequence,
                $isTransactional
            ));
        }

        return MemoryRecords::fromMessageSet(
            MessageSet::fromRecords($records, $compressionCodec, $messageFormatMagic)
        );
    }

    /**
     * Returns the `isolation.level` a Fetch request of this client states.
     *
     * The option is the one of the Java consumer - the strings `read_uncommitted` and `read_committed`, or the
     * wire value itself - and a client that does not configure it reads uncommitted, which is what every broker
     * below 0.11 did and what the versions below 4 of the Fetch api do.
     */
    private function isolationLevel(): int
    {
        $configured = $this->configuration['isolation.level'] ?? FetchRequest::READ_UNCOMMITTED;
        if (is_int($configured)) {
            return $configured;
        }

        return strtolower(trim((string) $configured)) === 'read_committed'
            ? FetchRequest::READ_COMMITTED
            : FetchRequest::READ_UNCOMMITTED;
    }

    /**
     * Checks whether the consumer offsets are stored in Kafka itself (OffsetCommit v3) instead of ZooKeeper (v0)
     */
    private function isOffsetStorageKafka(): bool
    {
        $storage = $this->configuration[ClientConfig::OFFSETS_STORAGE] ?? ClientConfig::OFFSETS_STORAGE_KAFKA;

        return $storage === ClientConfig::OFFSETS_STORAGE_KAFKA;
    }

    /**
     * Sends one request to a single known broker - the coordinator of a group, or the node an api like
     * ApiVersions addresses directly - and hands its answer to the given reader.
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
     * Sends one request of the group membership protocol and repeats it while the coordinator is not ready.
     *
     * These four apis report the state of the *coordinator* with the same three retriable error codes the
     * GroupCoordinator lookup uses - 14 GroupLoadInProgress while the coordinator reads the group out of
     * `__consumer_offsets`, 15 GroupCoordinatorNotAvailable while that topic is being created and 16
     * NotCoordinatorForGroup after the group moved to another broker - and none of them says anything about the
     * membership of the caller. They are therefore repeated here with the `retries` and `retry.backoff.ms` of
     * {@see RetryPolicy}, on top of the dropped-connection retry that {@see self::coordinatorRequest()} does; a
     * caller that still sees a 15 or a 16 afterwards has to look the coordinator up again.
     *
     * Every other error code - 22 IllegalGeneration, 23 InconsistentGroupProtocol, 25 UnknownMemberId, 26
     * InvalidSessionTimeout, 27 RebalanceInProgress, 30 GroupAuthorizationFailed - is a statement about this member
     * and is reported to the caller, which is the only one that can react to it.
     *
     * @template T
     *
     * @param Node                          $coordinatorNode Coordinator to talk to
     * @param Closure(int): AbstractRequest  $createRequest   Builds the request for a correlation id
     * @param class-string<AbstractResponse> $responseClass   Class of the expected response
     * @param Closure(mixed): T             $readResponse    Turns the response into the result
     *
     * @return T
     */
    private function groupRequest(
        Node $coordinatorNode,
        Closure $createRequest,
        string $responseClass,
        Closure $readResponse
    ): mixed {
        $policy = RetryPolicy::fromConfiguration($this->configuration);

        for ($attempt = 1;; $attempt++) {
            try {
                return $this->coordinatorRequest($coordinatorNode, $createRequest, $responseClass, $readResponse);
            } catch (
                GroupLoadInProgressException
                | GroupCoordinatorNotAvailableException
                | NotCoordinatorForGroupException $error
            ) {
                if ($attempt >= $policy->getMaxAttempts()) {
                    throw $error;
                }
                $policy->backoff();
            }
        }
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

    /**
     * Asks the controller to create the given topics (ApiKey 19, Kafka 0.10.1)
     *
     * The request goes out as CreateTopics v1, the highest version a 0.10.2.2 broker serves, so `$validateOnly` is
     * available and the answer carries the `error_message` of every topic that failed. Only the ACTIVE CONTROLLER
     * serves this api: `$controller` has to be the node that
     * {@see \Protocol\Kafka\Admin\AdminClient::findController()} returned, and a broker that is not (or is no
     * longer) the controller reports the error code 41 (NotController) for every topic of the request, which is
     * handed back as a {@see Common\Errors\NotControllerException} of that topic instead of being thrown - the
     * caller looks the controller up again and repeats the request.
     *
     * `$timeoutMs` is the time the controller waits for the topics to exist before it answers. A value of 0 answers
     * immediately, and every accepted topic then carries the error code 7 (RequestTimedOut) although its creation
     * has been scheduled and will finish shortly afterwards.
     *
     * @param Node           $controller   Active controller of the cluster
     * @param list<NewTopic> $newTopics    Topics to create
     * @param int            $timeoutMs    How long the controller waits for the topics to be created
     * @param bool           $validateOnly Validate the request without creating anything
     *
     * @return array<string, KafkaException|null> Error of every requested topic, null when it was created
     */
    public function createTopics(
        Node $controller,
        array $newTopics,
        int $timeoutMs = 30000,
        bool $validateOnly = false
    ): array {
        $clientId = (string) $this->configuration[ClientConfig::CLIENT_ID];
        $topics   = array_values($newTopics);

        return $this->controllerRequest(
            $controller,
            fn(int $correlationId): AbstractRequest => new CreateTopicsRequest(
                $topics,
                $timeoutMs,
                $validateOnly,
                $clientId,
                $correlationId
            ),
            CreateTopicsResponse::class,
            static function (CreateTopicsResponse $response) use ($topics): array {
                $result = [];
                foreach ($topics as $newTopic) {
                    $topicResult             = $response->topics[$newTopic->topic] ?? null;
                    $result[$newTopic->topic] = self::topicError(
                        $newTopic->topic,
                        $topicResult?->errorCode,
                        $topicResult?->errorMessage
                    );
                }

                return $result;
            }
        );
    }

    /**
     * Asks the controller to delete the given topics (ApiKey 20, Kafka 0.10.1)
     *
     * Deletion is asynchronous: `AdminUtils.deleteTopic` only marks the topic in ZooKeeper and the controller then
     * removes its partitions from the brokers, so `$timeoutMs` is how long the controller waits for that to finish
     * before it answers - a value of 0 answers immediately with the error code 7 (RequestTimedOut) for every topic
     * whose deletion was started. A topic that is unknown to the broker is reported with 3
     * (UnknownTopicOrPartition), and a broker that is not the active controller answers 41 (NotController) for
     * every topic, exactly like {@see self::createTopics()}.
     *
     * The api key exists whatever `delete.topic.enable` says; with the Kafka 0.10 default of `false` the topic is
     * accepted here and never actually removed.
     *
     * @param Node         $controller Active controller of the cluster
     * @param list<string> $topics     Names of the topics to delete
     * @param int          $timeoutMs  How long the controller waits for the topics to be deleted
     *
     * @return array<string, KafkaException|null> Error of every requested topic, null when it was deleted
     */
    public function deleteTopics(Node $controller, array $topics, int $timeoutMs = 30000): array
    {
        $clientId    = (string) $this->configuration[ClientConfig::CLIENT_ID];
        $topicNames  = array_values(array_map(strval(...), $topics));

        return $this->controllerRequest(
            $controller,
            fn(int $correlationId): AbstractRequest => new DeleteTopicsRequest(
                $topicNames,
                $timeoutMs,
                $clientId,
                $correlationId
            ),
            DeleteTopicsResponse::class,
            static function (DeleteTopicsResponse $response) use ($topicNames): array {
                $result = [];
                foreach ($topicNames as $topic) {
                    $result[$topic] = self::topicError($topic, $response->topics[$topic]->errorCode ?? null);
                }

                return $result;
            }
        );
    }

    /**
     * Sends one request of the topic administration apis to the active controller and hands its answer to a reader.
     *
     * The transport is the one of {@see self::coordinatorRequest()} - a single request to one named broker, with a
     * fresh correlation id, and with the `retries` and `retry.backoff.ms` of {@see RetryPolicy} for a connection
     * that dropped in between. The error codes of CreateTopics and DeleteTopics are reported per topic and never
     * repeated here: 41 NotController is the business of the caller, which has to look the controller up again.
     *
     * @template T
     *
     * @param Node                           $controller    Active controller of the cluster
     * @param Closure(int): AbstractRequest  $createRequest Builds the request for a correlation id
     * @param class-string<AbstractResponse> $responseClass Class of the expected response
     * @param Closure(mixed): T              $readResponse  Turns the response into the result
     *
     * @return T
     */
    private function controllerRequest(
        Node $controller,
        Closure $createRequest,
        string $responseClass,
        Closure $readResponse
    ): mixed {
        return $this->coordinatorRequest($controller, $createRequest, $responseClass, $readResponse);
    }

    /**
     * Turns the error code of one topic of a CreateTopics or DeleteTopics answer into the exception of the caller
     *
     * A topic that the controller did not report on at all is an answer this client can not interpret, so it
     * becomes an {@see UnknownErrorException} instead of a silent success.
     *
     * @param string      $topic        Name of the topic the entry belongs to
     * @param int|null    $errorCode    Error code of the topic, null when the answer has no entry for it
     * @param string|null $errorMessage Message the broker sent along with the code (CreateTopics v1 only)
     */
    private static function topicError(string $topic, ?int $errorCode, ?string $errorMessage = null): ?KafkaException
    {
        if ($errorCode === null) {
            return new UnknownErrorException(
                ['topic' => $topic, 'error' => 'The controller sent no result for this topic']
            );
        }
        if ($errorCode === KafkaException::NO_ERROR) {
            return null;
        }

        $context = ['topic' => $topic];
        if ($errorMessage !== null) {
            $context['error'] = $errorMessage;
        }

        return KafkaException::fromCode($errorCode, $context);
    }

    /**
     * Deletes the records before an offset of each of the given partitions (ApiKey 21, Kafka 0.11, KIP-107)
     *
     * The api moves the **low watermark** (`logStartOffset`) of a partition forward and leaves the deletion of the
     * segments below it to the log cleaner: everything BELOW the offset goes away, the record at the offset stays,
     * and the answer reports the new low watermark of each partition. {@see RecordsToDelete::HIGH_WATERMARK} (-1)
     * asks for everything that is fully replicated.
     *
     * Like Produce and Fetch this is served by the **leader** of each partition, so the request is split per leader
     * by {@see self::clusterRequest()} and the partitions that a metadata refresh can fix - 3, 5, 6 and a dropped
     * connection - are retried with the `retries` and `retry.backoff.ms` of {@see RetryPolicy}. A partition that
     * still fails afterwards is reported in the {@see TopicPartitionRequestException} together with the partial
     * result of the ones that succeeded, exactly like a produce or a fetch.
     *
     * @param array<string, array<int, int|RecordsToDelete>> $topicPartitionOffsets Offset to delete before, as
     *        topic => partition => offset
     * @param int $timeoutMs How long the leader waits for the new low watermark to be replicated, in milliseconds
     *
     * @return array<string, array<int, DeleteRecordsResponsePartition>> [topic => [partition => result]]
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     */
    public function deleteRecords(array $topicPartitionOffsets, int $timeoutMs = 30000): array
    {
        $partitionOffsets = [];
        foreach ($topicPartitionOffsets as $topic => $partitions) {
            foreach ($partitions as $partitionId => $offset) {
                $partitionOffsets[(string) $topic][(int) $partitionId] = $offset instanceof RecordsToDelete
                    ? $offset->beforeOffset
                    : (int) $offset;
            }
        }

        $clientId = (string) $this->configuration[ClientConfig::CLIENT_ID];

        return $this->clusterRequest(
            $partitionOffsets,
            fn(array $nodeTopicPartitions, int $correlationId): DeleteRecordsRequest => new DeleteRecordsRequest(
                $nodeTopicPartitions,
                $timeoutMs,
                $clientId,
                $correlationId
            ),
            DeleteRecordsResponse::class,
            static function (array $result, DeleteRecordsResponse $response, array &$errors): array {
                foreach ($response->topics as $topic => $topicResponse) {
                    /** @var DeleteRecordsResponsePartition $partitionResult */
                    foreach ($topicResponse->partitions as $partitionId => $partitionResult) {
                        if ($partitionResult->errorCode !== KafkaException::NO_ERROR) {
                            $errors[$topic][$partitionId] = KafkaException::fromCode(
                                $partitionResult->errorCode,
                                ['topic' => $topic, 'partitionId' => $partitionId]
                            );
                            continue;
                        }
                        $result[$topic][$partitionId] = $partitionResult;
                    }
                }

                return $result;
            }
        );
    }
}
