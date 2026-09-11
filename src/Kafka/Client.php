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
use Protocol\Kafka\Admin\CreatedTopic;
use Protocol\Kafka\Admin\ElectionType;
use Protocol\Kafka\Admin\NewPartitionReassignment;
use Protocol\Kafka\Admin\NewPartitions;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\PartitionReassignment;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\DuplicateSequenceNumberException;
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
use Protocol\Kafka\Consumer\Internals\FetchSessionHandler;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Consumer\OffsetAndTimestamp;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig as ProducerConfig;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResponsePartition;
use Protocol\Kafka\Protocol\Data\DeleteRecordsResponsePartition;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMember;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponseRecordError;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponse;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponse;
use Protocol\Kafka\Protocol\Request\AlterPartitionReassignmentsRequest;
use Protocol\Kafka\Protocol\Request\AlterPartitionReassignmentsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\CreatePartitionsRequest;
use Protocol\Kafka\Protocol\Request\CreatePartitionsResponse;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequest;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponse;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponse;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;
use Protocol\Kafka\Protocol\Request\ElectLeadersRequest;
use Protocol\Kafka\Protocol\Request\ElectLeadersResponse;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\EndTxnResponse;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequest;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\ListPartitionReassignmentsRequest;
use Protocol\Kafka\Protocol\Request\ListPartitionReassignmentsResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV4;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequest;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV2;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponse;
use Throwable;

/**
 * Low-level client for the Kafka protocol.
 *
 * Every api is sent with the highest version this line implements for it. **Kafka 2.0 raised every request-response
 * api by one** without changing a single byte of its frame (KIP-219): Produce goes out as **v6**, Fetch as **v8**,
 * Offsets (ListOffsets) as **v3** and Metadata as **v6**, where the 1.x line sent v5, v7, v2 and v5, and the group
 * apis one version up as well - GroupCoordinator **v2** and the membership apis. Kafka 2.1 to 2.3 then raised six
 * of those: OffsetCommit goes out as **v7**, with the `group_instance_id` of KIP-345, the `committed_leader_epoch`
 * of KIP-320 and without the `retention_time` that KIP-211 removed, OffsetFetch as **v5**, whose answer carries
 * that epoch back, JoinGroup as **v5**, whose first join is refused once with the member id the coordinator
 * assigns (KIP-394) unless it names a `group.instance.id` (KIP-345), SyncGroup and Heartbeat as **v3**, which
 * carry that instance id as well, and DescribeGroups as **v3**, which can ask for the operations the client may
 * perform on a group (KIP-430); GroupCoordinator, LeaveGroup, ListGroups and DeleteGroups stay at their KIP-219
 * versions. What those versions promise is what {@see self::awaitThrottle()} does - see the runtime note below.
 * Everything else is unchanged: Produce carries a record batch of the message format v2 and the transactional id of
 * its producer and its answer reports the `LogAppendTime` and the `LogStartOffset` of every partition, Fetch asks
 * for the log as it lies, bounds the whole answer with `fetch.max.bytes`, states the isolation level of the
 * consumer and can open an incremental fetch session, and OffsetCommit v0 is used when the offsets are stored in
 * ZooKeeper. The lower version classes of every api stay usable directly,
 * for a client that has to talk to an older broker - and `message.format.version` lowers the Produce request to v2
 * by itself, because a message set of the formats v0 and v1 has no place in a version 3 or higher request.
 *
 * **The one runtime change of Kafka 2.0 (KIP-219).** A broker that throttles a request answers it *first* and
 * **mutes the channel** for the `throttle_time_ms` it reports, instead of holding the answer back for that long -
 * and a 2.8.2 broker does that for every api version, the bumped ones only being how a client *states* that it
 * knows. This client therefore remembers the moment the throttle of each broker ends and sleeps whatever is left
 * of it before its next request to that broker, exactly as the Java `NetworkClient` does;
 * {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT} switches the waiting off and leaves the stall on
 * the broker side, where it makes the next throttle longer.
 *
 * Every request that addresses topic-partitions is split by their current leader and sent to all of those brokers
 * at once; the answers are collected with `stream_select()` as they arrive. A topic-partition whose leader answered
 * with a retriable error - 3 UnknownTopicOrPartition, 5 LeaderNotAvailable, 6 NotLeaderForPartition - or whose
 * connection dropped is sent again after the cluster metadata has been refreshed, up to `retries` times. What is
 * still broken afterwards is reported as a {@see TopicPartitionRequestException} that carries both the partial
 * result of the partitions that did succeed and the error of each partition that did not.
 *
 * @see docs/protocol/2.8.md
 */
class Client
{
    /**
     * Fallback for `request.timeout.ms` when the configuration does not carry it
     */
    private const int DEFAULT_REQUEST_TIMEOUT_MS = 30000;

    /**
     * How often {@see self::fetchPartitionsWithSessions()} answers a fetch session error with a full fetch of its
     * own, before it leaves the recovery to the next call
     *
     * This is not a retry of a failed request - a session error costs no partition anything and needs neither a
     * metadata refresh nor a backoff - it is the full fetch that the broker asked for by answering 70 or 71, and
     * one of them is always enough: it opens a new session and is answered with every partition of it.
     */
    private const int SESSION_ERROR_ATTEMPTS = 1;

    /**
     * Incremental fetch session of every broker this client fetched from with sessions, by node id
     *
     * @see self::fetchPartitionsWithSessions()
     *
     * @var array<int, FetchSessionHandler>
     */
    private array $fetchSessionHandlers = [];

    /**
     * Moment at which the throttle of a broker ends, as a UNIX timestamp with microseconds, by node id (KIP-219)
     *
     * An entry is written whenever an answer of that broker reported a `throttle_time_ms` above zero and is spent
     * - and removed - by the next request this client sends to the same broker, see {@see self::awaitThrottle()}.
     *
     * @see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT
     *
     * @var array<int, float>
     */
    private array $throttledUntil = [];

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
     * to probe it by sending a request of every key and version, because the api did not exist yet. A 2.8.2 broker
     * reports the **56** keys 0 to 51, 56, 57, 60 and 61 with the version ranges of the api-key table of the
     * protocol document, and answers before any authentication has happened on a SASL listener.
     *
     * The request goes out as **version 3**, the first flexible version of the protocol (Kafka 2.4): its frame
     * carries the request header v2 with a tag buffer and the compact `client_software_name` and
     * `client_software_version` of KIP-511 - `lisachenko-kafka-client` and the Kafka line this branch speaks - and
     * its answer is compact as well, with the features of KIP-584 as tagged fields behind the `throttleTimeMs`
     * that version 1 added. The version 2 below it is the KIP-219 bump of Kafka 2.0, which says that this client
     * honours a throttle time itself because a broker answers a throttled request of a bumped version **before** it
     * mutes the channel; version 3 inherits that promise.
     *
     * The client itself does **not** negotiate with the answer - like the `0.9.x` line it sends the fixed versions
     * that a broker of its own Kafka release serves - so this is an api for callers that want to know what they are
     * talking to, and the material a later line can build a negotiation on.
     *
     * The request is the one frame of the protocol whose *unsupported version* is answered instead of costing the
     * connection: a broker that does not know the version answers the error code 35 (UnsupportedVersion), since
     * Kafka 2.4 with the single api row of ApiVersions itself (KIP-511) and before it with an empty array. That
     * answer always arrives in the **version 0** layout, without the throttle time, so a peer older than Kafka 2.4
     * has to be asked with an {@see \Protocol\Kafka\Protocol\Request\ApiVersionsRequestV2},
     * {@see \Protocol\Kafka\Protocol\Request\ApiVersionsRequestV1} or
     * {@see \Protocol\Kafka\Protocol\Request\ApiVersionsRequestV0} and read with the response class of the same
     * version; this line speaks to a 2.8.2 broker, which serves v0 to v3.
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
     * The request goes out as **Produce v6** for the message format v2 (`message.format.version=0.11.0` and every
     * value above it, the default) and as Produce v2 for the legacy message sets of the formats v0 and v1, which
     * a version 3 request has no place for. Version 6 (Kafka 2.0, KIP-219) is the version 5 frame with another
     * number in its header; what it changes is that a throttled answer arrives immediately and the channel is
     * muted afterwards, which {@see self::awaitThrottle()} waits out.
     *
     * Every accepted partition carries three values the broker reported next to its base offset: the `logAppendTime` it stamped on the whole batch, which is -1 unless the topic is configured
     * with `message.timestamp.type=LogAppendTime`, the `logStartOffset` of the partition, which version 5 (Kafka
     * 1.0) appended to the answer and which a producer needs to tell a spurious `OutOfOrderSequence` from a real
     * one, and the `throttleTimeMs` of the answer it arrived in, which is 0 without a `producer_byte_rate` quota.
     * The versions 3, 4 and 6 added no field to the answer at all - `ProduceResponse.json` @ 2.8.2 has none of
     * them - so version 5 is the first one that reports the log start offset, and the version this client sends
     * carries the very same partition entry.
     *
     * An idempotent or transactional producer hands over its {@see TransactionManager}, which is the whole
     * difference between "at least once" and "exactly once, in order": the batch of every topic-partition is then
     * stamped with the producer id, the epoch and the sequence number that the manager keeps, the broker
     * recognises a batch it has already appended - one of the **last five** of that producer and partition, on a
     * 1.x broker - and answers it with the offset of that append instead of writing it twice, and the answer moves
     * the sequence numbers on. The three error codes of KIP-98 - 45, 46 and 47 - are reported to the manager,
     * which decides whether the producer starts over with a new producer id or is finished for good; a partition
     * that only failed with **46** (DuplicateSequenceNumber, "this batch is already in the log") counts as
     * accepted, with an unknown offset.
     *
     * The fourth code, **59** `UnknownProducerId` (Kafka 1.0), is the one this method answers itself: the broker
     * has no state of this producer for that partition any more, and
     * {@see TransactionManager::canRetryBatch()} decides on the `logStartOffset` of the answer whether that is
     * because every record of the producer was deleted from the log - a `DeleteRecords`, or a retention run - in
     * which case the partition is numbered from the sequence 0 again and the batch is **sent once more**, up to
     * `retries` times with `retry.backoff.ms` between the attempts. Only a 59 that this can not repair is reported
     * to the manager, which treats it as the out-of-order sequence it is a special case of.
     *
     * @param array<string, array<int, iterable<Record|string|\Stringable>>> $topicPartitionMessages Messages for
     *        each topic and partition
     * @param TransactionManager|null $transactionManager Producer state of an idempotent producer, `null` for a
     *        plain one, whose batches carry no producer id and are therefore not deduplicated by the broker
     *
     * @return array<string, array<int, ProduceResponsePartition>> Accepted partitions in the form
     *         [topic => [partition => ProduceResponsePartition]], empty for a fire-and-forget request (acks = 0),
     *         which the broker never answers
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     * @throws InvalidConfigurationException  For a `compression.type` or a `message.format.version` that this client
     *         can not write, and for a producer state next to an `acks` other than `all`
     */
    public function produce(array $topicPartitionMessages, ?TransactionManager $transactionManager = null): array
    {
        if ($transactionManager === null) {
            return $this->produceRecords($topicPartitionMessages);
        }

        // The sequence of a partition only moves on when the broker acknowledged the batch, so a request the
        // broker does not answer would send every batch with the sequence 0 and lose all of them but the first
        $acks = $this->configuration[ProducerConfig::ACKS] ?? ProducerConfig::ACKS_LEADER;
        if (ProducerConfig::parseAcks($acks) !== ProducerConfig::ACKS_ALL) {
            throw new InvalidConfigurationException(
                'The delivery guarantee of KIP-98 needs acks = all: a batch whose acknowledgement is not waited '
                . 'for can neither be deduplicated by the broker nor numbered by this producer'
            );
        }

        // The number of records of every partition is what its sequence number moves on by, and an `iterable` can
        // only be counted once it has been walked, so the batch is materialized before anything is sent
        $records = [];
        foreach ($topicPartitionMessages as $topic => $partitionMessages) {
            foreach ($partitionMessages as $partition => $messages) {
                $records[$topic][$partition] = self::toRecords($messages);
            }
        }

        // A produce with producer state IS an idempotent producer, and `enable.idempotence` refuses `retries = 0`
        // (ProducerConfig::resolveIdempotence()); a client that was handed a manager without going through the
        // producer configuration therefore still gets one further attempt. That is all the answer to a 59 needs:
        // the batch that is sent again starts at the sequence 0, and a first sequence of 0 is never a 59
        $retries   = max(1, (int) ($this->configuration[ProducerConfig::RETRIES] ?? 0));
        $backoffMs = (int) ($this->configuration[ClientConfig::RETRY_BACKOFF_MS] ?? 100);

        $pending  = $records;
        $accepted = [];
        $errors   = [];

        for ($attempt = 0; ; ++$attempt) {
            $producerIdAndEpoch = $transactionManager->maybeInitProducerId();
            $baseSequences      = $transactionManager->baseSequences($pending);

            $failures = [];
            try {
                $answered = $this->produceRecords(
                    $pending,
                    $producerIdAndEpoch->producerId,
                    $producerIdAndEpoch->epoch,
                    $baseSequences,
                    $transactionManager->getTransactionalId()
                );
            } catch (TopicPartitionRequestException $exception) {
                /** @var array<string, array<int, ProduceResponsePartition>> $answered */
                $answered = $exception->getPartialResult();
                $failures = $exception->getExceptions();
            }

            foreach ($answered as $topic => $partitions) {
                foreach ($partitions as $partitionId => $partitionInfo) {
                    $transactionManager->batchCompleted(
                        new TopicPartition((string) $topic, (int) $partitionId),
                        count($pending[$topic][$partitionId] ?? []),
                        $producerIdAndEpoch,
                        $partitionInfo->baseOffset
                    );
                    $accepted[$topic][$partitionId] = $partitionInfo;
                }
            }

            $retriable = [];
            foreach ($failures as $topic => $partitionErrors) {
                foreach ($partitionErrors as $partitionId => $error) {
                    $topicPartition = new TopicPartition((string) $topic, (int) $partitionId);
                    $recordCount    = count($pending[$topic][$partitionId] ?? []);

                    // Kafka 1.0: the broker lost the state of this producer for this partition (59). The
                    // `log_start_offset` of the Produce v5 answer decides whether sending the batch again can fix
                    // it - when it does, the manager has just numbered the partition from 0 again
                    if (
                        $attempt < $retries
                        && $transactionManager->canRetryBatch(
                            $topicPartition,
                            $error,
                            self::logStartOffsetOf($error),
                            $producerIdAndEpoch
                        )
                    ) {
                        $retriable[$topic][$partitionId] = $pending[$topic][$partitionId];
                        continue;
                    }

                    $transactionManager->batchFailed($topicPartition, $error, $recordCount, $producerIdAndEpoch);

                    // "The broker received a duplicate sequence number" is not a failure: the batch is in the log
                    // already, only the offset of that append is not in this answer
                    if ($error instanceof DuplicateSequenceNumberException) {
                        $accepted[$topic][$partitionId] = self::alreadyAppendedPartition((int) $partitionId);
                        continue;
                    }
                    $errors[$topic][$partitionId] = $error;
                }
            }

            if ($retriable === []) {
                break;
            }

            $pending = $retriable;
            if ($backoffMs > 0) {
                usleep($backoffMs * 1000);
            }
        }

        if ($errors !== []) {
            throw new TopicPartitionRequestException($accepted, $errors);
        }

        return $accepted;
    }

    /**
     * Returns the `log_start_offset` a failed partition of a Produce answer reported.
     *
     * The value travels in the context of the exception {@see Client::produceRecords()} builds out of the error
     * code, because the answer itself is gone by the time a producer decides what to do about it; an answer below
     * Produce v5, and an error that is not one of a produce request at all, answer
     * {@see ProduceResponsePartition::INVALID_OFFSET}.
     */
    private static function logStartOffsetOf(Throwable $error): int
    {
        if (!$error instanceof KafkaException) {
            return ProduceResponsePartition::INVALID_OFFSET;
        }

        return (int) ($error->getContext()['logStartOffset'] ?? ProduceResponsePartition::INVALID_OFFSET);
    }

    /**
     * Asks a broker for a producer id and its epoch (ApiKey 22, Kafka 0.11, KIP-98)
     *
     * This is the first request of an idempotent and of a transactional producer, and the transactional id decides
     * both what it means and **which broker has to answer it**:
     *
     * * with `null` - the idempotent producer - any broker of the cluster hands out the next producer id of the
     *   block it reserved in ZooKeeper, with the epoch 0. Every call answers a new id, so a producer asks once and
     *   keeps what it got until it is closed;
     * * with a transactional id, the request goes to the **transaction coordinator** of that id, which
     *   {@see Client::getTransactionCoordinator()} looks up: the answer is the producer id that
     *   `__transaction_state` holds for the id, with an epoch that is one higher than the one the previous producer
     *   of that id used, which fences that producer.
     *
     * The `transactionTimeoutMs` is what the coordinator waits for a status update of an open transaction before it
     * aborts it, and it is only checked for a non-null id: above the broker's `transaction.max.timeout.ms` (900000
     * by default) the answer is the error code 50 (InvalidTransactionTimeout), while a `null` transactional id is
     * answered with a producer id whatever the value is.
     *
     * @param string|null $transactionalId      Transactional id of the producer, `null` for an idempotent one
     * @param int         $transactionTimeoutMs `transaction.timeout.ms` of the producer, ignored without an id
     *
     * @throws Common\Errors\InvalidTxnTimeoutException For a timeout above `transaction.max.timeout.ms`
     * @throws Common\Errors\InvalidRequestException    For the empty string as a transactional id
     * @throws KafkaException                           For every other error code of the answer
     */
    public function initProducerId(
        ?string $transactionalId = null,
        int $transactionTimeoutMs = InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS
    ): ProducerIdAndEpoch {
        // A producer id without a transactional id is not coordinated by anything, so any broker may answer it
        $node = $transactionalId === null
            ? self::anyNodeOf($this->cluster)
            : $this->getTransactionCoordinator($transactionalId);

        return $this->coordinatorRequest(
            $node,
            fn(int $correlationId): InitProducerIdRequest => new InitProducerIdRequest(
                $transactionalId,
                $transactionTimeoutMs,
                $this->configuration[ClientConfig::CLIENT_ID],
                $correlationId
            ),
            InitProducerIdResponse::class,
            static function (InitProducerIdResponse $response) use ($transactionalId): ProducerIdAndEpoch {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['transactionalId' => $transactionalId]
                    );
                }

                return new ProducerIdAndEpoch($response->producerId, $response->producerEpoch);
            }
        );
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

        // The wire format carries one opaque record set per topic-partition, see docs/protocol/2.8.md
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

        // A message set of the formats v0 and v1 can only be sent with a version below 3, which is also the
        // highest version that has no place for a transactional id; the message format v2 goes out as Produce v6,
        // the version Kafka 2.0 bumped the api to (KIP-219), whose answer is the version 5 frame - the first one
        // that reports the log start offset of every partition
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
                            // The log start offset travels with the *error* as well, because it is what decides
                            // what a producer does about a 59 (UnknownProducerId): the field is the only way to
                            // tell the head of the log being deleted under a producer from a real out-of-order
                            // sequence, see TransactionManager::canRetryBatch()
                            $context = [
                                'topic'          => $topic,
                                'partitionId'    => $partitionId,
                                'logStartOffset' => $partitionInfo->logStartOffset,
                            ];
                            // The record errors of KIP-467 (Produce v8): which records of the sent batch the
                            // broker refused, and why. They travel into the exception, because "the batch was
                            // refused" without them is what every version below 8 already said
                            if ($partitionInfo->errorMessage !== null) {
                                $context['errorMessage'] = $partitionInfo->errorMessage;
                            }
                            if ($partitionInfo->recordErrors !== []) {
                                $context['recordErrors'] = array_map(
                                    static fn(ProduceResponseRecordError $recordError): ?string
                                        => $recordError->batchIndexErrorMessage,
                                    $partitionInfo->recordErrors
                                );
                            }

                            $errors[$topic][$partitionId] = KafkaException::fromCode(
                                $partitionInfo->errorCode,
                                $context
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
     * The request goes out as **Fetch v7**, which means five things:
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
     * * the request carries the `session_id 0` / `epoch -1` of {@see FetchMetadata::legacy()}, i.e. it opens no
     *   incremental fetch session (KIP-227), and a 1.1.1 broker serves it exactly as it serves a Fetch v6: the
     *   whole requested set comes back, and the answer reports the session id 0 and the top-level error code 0.
     *   {@see FetchRequest} implements the whole frame, so a caller that wants a session builds the request itself;
     * * every partition of the answer reports its `lastStableOffset`, its `logStartOffset` and the transactions
     *   that were aborted in the range it covers, and the request states the `isolation.level` of the consumer -
     *   `read_uncommitted` unless it is configured otherwise, which is what the broker answers a -1 last stable
     *   offset and a `null` aborted-transactions array to.
     *
     * @param array<string, array<int, int|array{int, int}>> $topicPartitionOffsets Offset to start fetching each
     *        partition at. A value may also be the pair `[offset, currentLeaderEpoch]`, which is the epoch that
     *        **Fetch v9** (Kafka 2.1, KIP-320) puts on the wire and that fences a consumer whose metadata is out
     *        of date with 74 or 75; a plain integer means "I do not know the epoch", which is what every call
     *        written before Kafka 2.1 means.
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
            static fn(array $result, FetchResponse $response, array &$errors): array => self::collectFetchedPartitions(
                $result,
                $response,
                $topicPartitionOffsets,
                $checkCrcs,
                $errors
            ),
            $timeout
        );
    }

    /**
     * Fetches messages the way a consumer does: with an **incremental fetch session** per broker (KIP-227)
     *
     * The frame, the isolation level and everything that is read out of an answer are the ones of
     * {@see self::fetchPartitions()}; what is different is what travels in the request and what comes back:
     *
     * * the **first** request to a broker is a full fetch with the epoch 0, which asks it to open a fetch session
     *   and is answered with the id of that session and with every partition of the request;
     * * every following request to it is an **incremental** fetch of that session and states only the partitions
     *   whose fetch offset moved since the last one - the broker remembers the others. A partition that is not in
     *   `$topicPartitionOffsets` any more (a rebalance took it away, {@see \Protocol\Kafka\Consumer\KafkaConsumer}
     *   paused it, its topic was deleted) travels in the `forgotten_topics_data` of that request and is dropped
     *   from the session;
     * * the answer of an incremental fetch carries **only the partitions that have news**, up to an answer with no
     *   topic at all, so the returned array holds only those. A partition that is missing from it has nothing new:
     *   its position, its high water mark and everything else the caller knows about it are still valid, which is
     *   why this method reports what came back instead of an entry per requested partition.
     *
     * The session state lives in one {@see FetchSessionHandler} per node ({@see self::getFetchSessionHandlers()}),
     * which also does the recovery: a broker that answers **70** `FetchSessionIdNotFound` - it evicted the session,
     * its cache holds 1000 of them - or **71** `InvalidFetchSessionEpoch` - a request or an answer was lost - is
     * asked again with a full fetch right away, so that a caller never sees a session error; the same happens when
     * a connection drops before the answer arrives. A partition that a broker answers with a retriable error is
     * handled as in {@see self::fetchPartitions()}, except that the request that follows the metadata refresh
     * carries the **whole** set again: an incremental request that carried the failed partitions alone would tell
     * the broker to forget all the others.
     *
     * @param array<string, array<int, int|array{int, int}>> $topicPartitionOffsets Offset to start fetching each
     *        partition at, optionally as the pair `[offset, currentLeaderEpoch]` of KIP-320, see
     *        {@see self::fetchPartitions()}
     * @param int                            $timeout               Timeout in ms to wait for fetching
     *
     * @return array<string, array<int, FetchedPartition>> [topic => [partition => FetchedPartition]], only the
     *         partitions the brokers answered
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     */
    public function fetchPartitionsWithSessions(array $topicPartitionOffsets, int $timeout): array
    {
        $timeout        = (int) min($this->configuration[ConsumerConfig::FETCH_MAX_WAIT_MS], $timeout);
        $checkCrcs      = (bool) ($this->configuration[ConsumerConfig::CHECK_CRCS] ?? true);
        $isolationLevel = $this->isolationLevel();
        $policy         = RetryPolicy::fromConfiguration($this->configuration);
        $maxAttempts    = $policy->getMaxAttempts();

        $result          = [];
        $permanentErrors = [];
        $pending         = $topicPartitionOffsets;
        $attempt         = 1;
        $sessionAttempts = self::SESSION_ERROR_ATTEMPTS;

        while (true) {
            $errors         = [];
            $sessionErrors  = [];
            $requestedNodes = [];
            $answeredNodes  = [];

            $round = $this->dispatch(
                $pending,
                function (array $nodeTopicRequest, int $correlationId, int $nodeId) use (
                    &$requestedNodes,
                    $timeout,
                    $isolationLevel
                ): FetchRequest {
                    $handler = $this->fetchSessionHandlers[$nodeId] ??= new FetchSessionHandler($nodeId);
                    $builder = $handler->newBuilder();
                    foreach ($nodeTopicRequest as $topic => $partitionOffsets) {
                        foreach ($partitionOffsets as $partitionId => $fetchOffset) {
                            // The value travels into the session as it was given - a plain offset, or the pair
                            // [offset, currentLeaderEpoch] of KIP-320 - because the session compares it with the
                            // one it remembers to decide whether the partition has to be sent again at all
                            $builder->add(
                                new TopicPartition((string) $topic, (int) $partitionId),
                                is_array($fetchOffset) ? [(int) $fetchOffset[0], (int) $fetchOffset[1]] : (int) $fetchOffset
                            );
                        }
                    }
                    $requestData             = $builder->build();
                    $requestedNodes[$nodeId] = true;

                    return new FetchRequest(
                        $requestData->toSend,
                        $timeout,
                        $this->configuration[ConsumerConfig::FETCH_MIN_BYTES],
                        $this->configuration[ConsumerConfig::MAX_PARTITION_FETCH_BYTES],
                        -1,
                        $this->configuration[ConsumerConfig::CLIENT_ID],
                        $correlationId,
                        (int) ($this->configuration[ConsumerConfig::FETCH_MAX_BYTES]
                            ?? FetchRequest::DEFAULT_MAX_BYTES),
                        $isolationLevel,
                        $requestData->metadata,
                        $requestData->toForget
                    );
                },
                FetchResponse::class,
                function (array $result, FetchResponse $response, array &$errors, int $nodeId) use (
                    $pending,
                    $checkCrcs,
                    &$answeredNodes,
                    &$sessionErrors
                ): array {
                    $answeredNodes[$nodeId] = true;
                    $handler                = $this->fetchSessionHandlers[$nodeId] ?? null;
                    if ($handler !== null && !$handler->handleResponse($response)) {
                        // A session error - 70 or 71 - is answered with an empty topics array and costs no
                        // partition anything; the handler is back at a full fetch, which is sent right away
                        $sessionErrors[$nodeId] = $response->errorCode;

                        return $result;
                    }

                    return self::collectFetchedPartitions($result, $response, $pending, $checkCrcs, $errors);
                },
                $timeout,
                $errors
            );
            $result = self::mergeResult($result, $round);

            // A broker that never answered leaves its session in an unknown state: the next request to it closes
            // whatever is left of it and opens a new one
            foreach (array_keys($requestedNodes) as $nodeId) {
                if (!isset($answeredNodes[$nodeId])) {
                    $this->fetchSessionHandlers[$nodeId]->handleError();
                }
            }

            $hasRetriable = false;
            foreach ($errors as $topic => $partitionErrors) {
                foreach ($partitionErrors as $partitionId => $error) {
                    $canRetry = $attempt < $maxAttempts
                        && RetryPolicy::isRetriable($error)
                        && isset($pending[$topic][$partitionId]);
                    if ($canRetry) {
                        $hasRetriable = true;
                    } else {
                        $permanentErrors[$topic][$partitionId] = $error;
                    }
                }
            }

            if ($hasRetriable) {
                // The error codes 3, 5, 6 and a dropped connection all mean the same thing: the leader this client
                // has in its metadata is not the leader of that partition any more
                $this->reloadCluster();
                $policy->backoff();
                $attempt++;
            } elseif ($sessionErrors !== [] && $sessionAttempts > 0) {
                $sessionAttempts--;
            } else {
                break;
            }

            $pending = self::withoutPartitions($topicPartitionOffsets, $permanentErrors);
            if ($pending === []) {
                break;
            }
        }

        if ($permanentErrors !== []) {
            throw new TopicPartitionRequestException($result, $permanentErrors);
        }

        return $result;
    }

    /**
     * Returns the incremental fetch session this client holds with every broker it fetched from, by node id
     *
     * @return array<int, FetchSessionHandler>
     */
    public function getFetchSessionHandlers(): array
    {
        return $this->fetchSessionHandlers;
    }

    /**
     * Merges one Fetch answer into the result of a fetch, reporting the error of every partition that failed
     *
     * @param array<string, array<int, FetchedPartition>> $result                Partitions that are already known
     * @param FetchResponse                               $response              Answer of one broker
     * @param array<string, array<int, int>>              $topicPartitionOffsets Offset every partition was asked at
     * @param bool                                        $checkCrcs             Whether to verify every checksum
     * @param array<string, array<int, Exception>>        $errors                Collects the error of each partition
     *
     * @return array<string, array<int, FetchedPartition>>
     */
    private static function collectFetchedPartitions(
        array $result,
        FetchResponse $response,
        array $topicPartitionOffsets,
        bool $checkCrcs,
        array &$errors
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
                // A requested offset may be the pair [offset, currentLeaderEpoch] of KIP-320, see
                // FetchRequest::offsetAndEpoch(); only the offset itself is of interest here
                [$fetchOffset] = FetchRequest::offsetAndEpoch($topicPartitionOffsets[$topic][$partitionId] ?? 0);
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
                    $responsePartition->abortedTransactions,
                    $responsePartition->preferredReadReplica
                );
            }
        }

        return $result;
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
     * @param array<string, array<int, int|array{int, int}>> $topicPartitionTimestamps Target time of each topic
     *        partition, optionally as the pair `[timestamp, currentLeaderEpoch]` that version 4 (KIP-320) puts on
     *        the wire
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
                $this->isolationLevel(),
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
                            : new OffsetAndTimestamp(
                                $partitionMetadata->offset,
                                $partitionMetadata->timestamp,
                                $partitionMetadata->leaderEpoch === OffsetsResponsePartition::UNKNOWN_LEADER_EPOCH
                                    ? null
                                    : $partitionMetadata->leaderEpoch
                            );
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Asks the leader of every named partition where a leader epoch of its log ended (api key 23, **v2**)
     *
     * This is the wire half of the truncation detection of **KIP-320**. A consumer that has seen a *new* leader
     * epoch for a partition does not know whether the records it was about to read survived the leader change, so
     * it asks the new leader: "you took over from epoch `e` - which offset does that epoch end at?". An
     * `end_offset` **below** the consumer's position means that the log diverged there and the position has to be
     * moved back; an offset at or above it means that the position is still inside a part of the log the new
     * leader has, and the consumer goes on reading.
     *
     * The request goes out as **version 2** (Kafka 2.1), which carries the `current_leader_epoch` that fences it:
     * a stale belief about the leadership is answered **74** `FENCED_LEADER_EPOCH` and a belief from the future
     * **75** `UNKNOWN_LEADER_EPOCH`, both of which mean "refresh the metadata and ask again" rather than "move the
     * position". The api answers an ordinary client just as it answers a follower, because
     * `KafkaApis.handleOffsetForLeaderEpochRequest` @ 2.8.2 authorizes it with `ClusterAction on Cluster`, which a
     * broker without an `authorizer.class.name` grants to everybody.
     *
     * @param array<string, array<int, int|array{int, int}>> $topicPartitionEpochs Epoch to resolve per partition,
     *        as topic => partition => epoch, or as the pair `[leaderEpoch, currentLeaderEpoch]`
     *
     * @return array<string, array<int, OffsetForLeaderEpochResponsePartition>> The answer of every partition
     *
     * @throws TopicPartitionRequestException If a partition was answered with an error code
     */
    public function offsetsForLeaderEpochs(array $topicPartitionEpochs): array
    {
        return $this->clusterRequest(
            $topicPartitionEpochs,
            fn(array $nodeTopicRequest, int $correlationId): OffsetForLeaderEpochRequest
                => new OffsetForLeaderEpochRequest(
                    $nodeTopicRequest,
                    $this->configuration[ClientConfig::CLIENT_ID],
                    $correlationId
                ),
            OffsetForLeaderEpochResponse::class,
            static function (array $result, OffsetForLeaderEpochResponse $response, array &$errors): array {
                foreach ($response->topics as $topic => $topicResponse) {
                    /** @var OffsetForLeaderEpochResponsePartition $partition */
                    foreach ($topicResponse->partitions as $partitionId => $partition) {
                        if ($partition->errorCode !== KafkaException::NO_ERROR) {
                            $errors[$topic][$partitionId] = KafkaException::fromCode(
                                $partition->errorCode,
                                ['topic' => $topic, 'partitionId' => $partitionId]
                            );
                            continue;
                        }
                        $result[$topic][$partitionId] = $partition;
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Commits the offsets for topic partitions for the concrete consumer group
     *
     * The version of the request follows the `offsets.storage` option: version 6 stores the offsets in the
     * `__consumer_offsets` topic of the cluster and has to be sent to the coordinator of the group, version 0 stores
     * them in ZooKeeper and is answered by any broker. An offset may be given as a plain integer or as an
     * {@see OffsetAndMetadata}, which the broker keeps and hands back with the next OffsetFetch - and whose
     * `leaderEpoch` travels in the `committed_leader_epoch` of the v6 partition entry (KIP-320, Kafka 2.1); a
     * plain integer, or an {@see OffsetAndMetadata} without an epoch, commits
     * {@see \Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartition::UNKNOWN_LEADER_EPOCH}.
     *
     * **`$retentionTimeMs` no longer reaches the wire.** It is the `retention_time` field of the versions 2 to 4,
     * and KIP-211 (Kafka 2.1) removed it from version 5 on, because the committed offsets of a group expire
     * `offsets.retention.minutes` after the **group** became empty from that release on. Pass it to
     * {@see OffsetCommitRequestV4} directly to reach a broker that still reads it; the ZooKeeper version never had
     * the field either. A client that is not a member of a group commits with
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
     * @param string|null                                      $groupInstanceId       `group.instance.id` of a
     *        static member (KIP-345, version 7), null for a dynamic one
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
        int $retentionTimeMs,
        ?string $groupInstanceId = null
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
                    $correlationId,
                    $groupInstanceId
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
     * - `kafka` reads them out of `__consumer_offsets` with the version 4 of the api, `zookeeper` with the version 0.
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
     * A client that has no member id yet passes {@see JoinGroupRequest::DEFAULT_MEMBER_ID}; a member that rejoins
     * has to pass the id of the previous generation. The answer names the generation, the protocol the coordinator
     * picked out of `$groupProtocols` and the leader of the group - the member whose id equals the `leaderId` of
     * the answer is the one that computes the assignment and publishes it with {@see self::syncGroup()}; only that
     * member receives the `members` array.
     *
     * **A first join is refused once** (KIP-394, Kafka 2.2): the version 4 request this client sends is answered
     * with the error code 79 and the member id the coordinator assigned, which is reported as a
     * {@see Common\Errors\MemberIdRequiredException} whose context carries that id under `assignedMemberId`. The
     * caller sends the very same request again with it - {@see \Protocol\Kafka\Consumer\Internals\ConsumerCoordinator}
     * does it immediately and without a backoff, as the Java `AbstractCoordinator.handleJoinResponse` does.
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
     * @param string|null           $groupInstanceId   `group.instance.id` of a static member (KIP-345, version 5),
     *        null for a dynamic one
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
        ?int $rebalanceTimeoutMs = null,
        ?string $groupInstanceId = null
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
                $correlationId,
                $groupInstanceId
            ),
            JoinGroupResponse::class,
            static function (JoinGroupResponse $response) use ($groupId, $memberId, $protocolType): JoinGroupResponse {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    // The 79 of KIP-394 is the one error answer that carries something the caller needs: the
                    // member id the coordinator assigned, which the next join has to send. It travels in the
                    // context under `assignedMemberId`, next to the (empty) id this request was sent with.
                    $context = ['groupId' => $groupId, 'memberId' => $memberId, 'protocolType' => $protocolType];
                    if ($response->errorCode === KafkaException::MEMBER_ID_REQUIRED) {
                        $context['assignedMemberId'] = $response->memberId;
                    }

                    throw KafkaException::fromCode($response->errorCode, $context);
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
     * @param string|null           $groupInstanceId  `group.instance.id` of a static member (KIP-345, version 3),
     *        null for a dynamic one
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
        array $groupAssignments = [],
        ?string $groupInstanceId = null
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
                $correlationId,
                $groupInstanceId
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
     * @param string|null $groupInstanceId `group.instance.id` of a static member (KIP-345, version 3), null for a
     *        dynamic one; a heartbeat that names an instance id another consumer has taken over is answered 82
     *        ({@see Common\Errors\FencedInstanceIdException}), which is fatal for this member
     *
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     * @throws Common\Errors\IllegalGenerationException
     * @throws Common\Errors\UnknownMemberIdException
     * @throws Common\Errors\RebalanceInProgressException
     * @throws Common\Errors\FencedInstanceIdException
     * @throws Common\Errors\GroupAuthorizationFailedException
     */
    public function heartbeat(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        int $generationId,
        ?string $groupInstanceId = null
    ): void {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        $this->groupRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => new HeartbeatRequest(
                $groupId,
                $generationId,
                $memberId,
                $clientId,
                $correlationId,
                $groupInstanceId
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
     * **Version 3 (Kafka 2.4, KIP-345) turned the request into a batch**, and a member that removes itself is that
     * batch with exactly one entry. The error of that member then travels in the member array of the answer while
     * the top-level error code stays 0, so both are checked here and the member error is reported the way it
     * always was - 25 (`UnknownMemberId`) for a member the group does not have, 82 (`FencedInstanceId`) for a
     * static member whose instance id another consumer has taken over. Several members at once are what
     * {@see \Protocol\Kafka\Admin\AdminClient::removeMembersFromConsumerGroup()} sends.
     *
     * @param Node        $coordinatorNode Current group coordinator for $groupId
     * @param string      $groupId         Name of the group
     * @param string      $memberId        Name of the group member
     * @param string|null $groupInstanceId `group.instance.id` of a static member (KIP-345, version 3), null for a
     *        dynamic one; naming both makes the coordinator check that the member id belongs to that instance
     *
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     * @throws Common\Errors\UnknownMemberIdException
     * @throws Common\Errors\FencedInstanceIdException
     * @throws Common\Errors\GroupAuthorizationFailedException
     */
    public function leaveGroup(
        Node $coordinatorNode,
        string $groupId,
        string $memberId,
        ?string $groupInstanceId = null
    ): void {
        $clientId = (string) $this->configuration[ConsumerConfig::CLIENT_ID];

        $this->groupRequest(
            $coordinatorNode,
            fn(int $correlationId): AbstractRequest => new LeaveGroupRequest(
                $groupId,
                [new LeaveGroupRequestMember($memberId, $groupInstanceId)],
                $clientId,
                $correlationId
            ),
            LeaveGroupResponse::class,
            static function (LeaveGroupResponse $response) use ($groupId, $memberId): void {
                $context = ['groupId' => $groupId, 'memberId' => $memberId];
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode($response->errorCode, $context);
                }
                foreach ($response->members as $member) {
                    if ($member->errorCode !== KafkaException::NO_ERROR) {
                        throw KafkaException::fromCode($member->errorCode, $context);
                    }
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
     * Returns a broker of the cluster for a request that any of them may answer, e.g. an `InitProducerId` without
     * a transactional id.
     *
     * The Java client picks its least loaded node here; this one has no in-flight bookkeeping to pick by and sends
     * such a request to the **first broker of the cluster metadata**, deterministically. A broker that does not
     * answer costs the request, which the caller repeats after a metadata refresh - a producer id is asked for
     * once per session, so there is nothing to spread over the cluster.
     *
     * @throws NetworkException If the cluster metadata list no broker at all
     */
    private static function anyNodeOf(Cluster $cluster): Node
    {
        $nodes = $cluster->nodes();
        if ($nodes === []) {
            throw new NetworkException(['error' => 'The cluster metadata list no broker to send the request to']);
        }

        return $nodes[array_key_first($nodes)];
    }

    /**
     * Builds the answer of a partition whose batch the broker reported as one it already holds (error code 46).
     *
     * The base offset of that append is not part of a DuplicateSequenceNumber answer - only the answer that a
     * 0.11.0.3 broker really sends for a duplicate, the error code 0 with the original offset, carries it - so the
     * partition is reported as accepted at an unknown offset, the -1 that every official client uses for it.
     */
    private static function alreadyAppendedPartition(int $partitionId): ProduceResponsePartition
    {
        $partitionResult             = new ProduceResponsePartition();
        $partitionResult->partition  = $partitionId;
        $partitionResult->errorCode  = KafkaException::NO_ERROR;
        $partitionResult->baseOffset = -1;

        return $partitionResult;
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
     * Returns the `isolation.level` that a Fetch and an Offsets request of this client state.
     *
     * The option is the one of the Java consumer ({@see ConsumerConfig::ISOLATION_LEVEL}) - the strings
     * `read_uncommitted` and `read_committed`, or the wire value itself - and a client that does not configure it
     * reads uncommitted, which is what every broker below 0.11 did and what the versions below 4 of the Fetch api
     * do.
     *
     * The level travels in **both** apis on purpose: a `read_committed` consumer whose `endOffsets()` answered the
     * high watermark would wait for records that it is never going to be shown, so version 2 of the Offsets api
     * (KIP-98) carries the field as well and answers the last stable offset for `LATEST`.
     */
    private function isolationLevel(): int
    {
        $configured = $this->configuration[ConsumerConfig::ISOLATION_LEVEL] ?? FetchRequest::READ_UNCOMMITTED;
        if (is_int($configured)) {
            return $configured;
        }

        return strtolower(trim((string) $configured)) === ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED
            ? FetchRequest::READ_COMMITTED
            : FetchRequest::READ_UNCOMMITTED;
    }

    /**
     * Checks whether the consumer offsets are stored in Kafka itself (OffsetCommit v4) instead of ZooKeeper (v0)
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

            // KIP-219: the channel of a broker that throttled the last answer is muted, see self::awaitThrottle()
            $this->awaitThrottle($coordinatorNode->nodeId);
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
            $this->recordThrottleTime($coordinatorNode->nodeId, $response);

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
            // An acks = 0 request is never answered, so it can never learn of a throttle itself - but a throttle
            // that an earlier answer of this broker reported still mutes the channel, and is waited out here
            $this->awaitThrottle($nodeId);
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
     * Both closures are called with the id of the node they belong to as their last argument, which is what a
     * request that carries per-broker state - the incremental fetch session of {@see FetchSessionHandler} - needs
     * to find that state again; a closure that does not care about it simply declares fewer parameters.
     *
     * @param array<string, array<int, mixed>>          $topicPartitionsRequest Request data per topic-partition
     * @param Closure(array, int, int): AbstractRequest $nodeRequest            Builds the request of one node
     * @param class-string<AbstractResponse> $responseClass          Class of the expected response
     * @param Closure(array, mixed, array, int): array  $responseAggregator     Merges one answer into the result
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
                $request       = $nodeRequest($nodeTopicPartitions, $correlationId, $nodeId);
                $stream        = $this->connectionTo($nodeId);
                // KIP-219: a broker that throttled the last answer has muted this channel, so the request is
                // held back until the reported delay has passed instead of being written into the mute
                $this->awaitThrottle($nodeId);
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
                $this->recordThrottleTime($nodeId, $responses[$nodeId]);
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
        foreach ($responses as $nodeId => $response) {
            $result = $responseAggregator($result, $response, $exceptions, $nodeId);
        }

        return $result;
    }

    /**
     * Waits out whatever is left of the throttle a broker imposed on this client, before the next request to it.
     *
     * This is the client half of **KIP-219** (Kafka 2.0), which the versions Produce v6, Fetch v8, Offsets v3 and
     * Metadata v6 announce: a broker that throttles a request answers it **immediately**, with the delay it is
     * about to impose in `throttle_time_ms`, and mutes the channel for that long afterwards, where a broker below
     * Kafka 2.0 simply held the answer back for the same time. A client that writes its next request right away
     * therefore does not get served any earlier - it writes into a muted connection, waits out the mute anyway and
     * makes the next throttle longer - which is why this method sleeps the remaining time first, exactly as the
     * Java `NetworkClient` does.
     *
     * What is remembered is the **deadline**, not the value: a caller that spent the throttle time doing
     * something else does not wait for it twice, and the entry is spent by the first request that follows the
     * throttled answer. {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT} switches the waiting off,
     * which leaves the stall on the broker side, as every line of this package below 2.0 had it.
     *
     * The metadata refresh of {@see Cluster} does not pass through here: it opens its own connection to any
     * broker of the bootstrap list and is not part of this client's per-node bookkeeping.
     *
     * @param int $nodeId Broker the next request goes to
     */
    private function awaitThrottle(int $nodeId): void
    {
        $deadline = $this->throttledUntil[$nodeId] ?? null;
        if ($deadline === null) {
            return;
        }

        unset($this->throttledUntil[$nodeId]);
        // Rounded to whole microseconds, which is the resolution of usleep(): the product of two floats is off
        // by a few nanoseconds and would otherwise turn a 400 ms throttle into 400001 microseconds of sleep
        $remainingMicroseconds = (int) round(($deadline - $this->currentTime()) * 1000000);
        if ($remainingMicroseconds > 0) {
            $this->sleepFor($remainingMicroseconds);
        }
    }

    /**
     * Remembers when the throttle that an answer reports ends, so that the next request to that broker waits.
     *
     * Nothing is remembered when the answer carries no throttle time, when it reports zero - the answer of every
     * broker without a quota - or when {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT} is off.
     *
     * @param int              $nodeId   Broker that sent the answer
     * @param AbstractResponse $response Answer that was just read
     */
    private function recordThrottleTime(int $nodeId, AbstractResponse $response): void
    {
        if (!($this->configuration[ClientConfig::THROTTLE_WAIT] ?? true)) {
            return;
        }

        $throttleTimeMs = self::throttleTimeOf($response);
        if ($throttleTimeMs > 0) {
            $this->throttledUntil[$nodeId] = $this->currentTime() + $throttleTimeMs / 1000;
        }
    }

    /**
     * Reads the `throttle_time_ms` of any answer, whatever the property of its class is called.
     *
     * The field has two names in this package, because the apis spell it differently on the wire and the classes
     * follow the spec: the Produce answer reports `ThrottleTime` **behind** its topics array
     * ({@see ProduceResponse::$throttleTime}), every other api carries `throttle_time_ms` in front of its body
     * ({@see FetchResponse::$throttleTimeMs}). An answer of a version that predates KIP-124 has neither.
     */
    private static function throttleTimeOf(AbstractResponse $response): int
    {
        foreach (['throttleTimeMs', 'throttleTime'] as $property) {
            if (property_exists($response, $property)) {
                return (int) $response->$property;
            }
        }

        return 0;
    }

    /**
     * Returns the current time as a UNIX timestamp with microseconds; a test double replaces the clock here
     */
    protected function currentTime(): float
    {
        return microtime(true);
    }

    /**
     * Sleeps for the given number of microseconds; a test double replaces the sleep here
     */
    protected function sleepFor(int $microseconds): void
    {
        usleep($microseconds);
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
     * Returns the given topic-partitions without the ones the second array holds
     *
     * @param array<string, array<int, mixed>> $topicPartitions Partitions to filter
     * @param array<string, array<int, mixed>> $toRemove        Partitions to leave out
     *
     * @return array<string, array<int, mixed>>
     */
    private static function withoutPartitions(array $topicPartitions, array $toRemove): array
    {
        foreach ($toRemove as $topic => $partitions) {
            foreach (array_keys($partitions) as $partitionId) {
                unset($topicPartitions[$topic][$partitionId]);
            }
            if (($topicPartitions[$topic] ?? null) === []) {
                unset($topicPartitions[$topic]);
            }
        }

        return $topicPartitions;
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
     * @return array<string, CreatedTopic> What the controller answered for every requested topic: its error, and
     *         from the version 5 of the api the partition count, the replication factor and the configuration the
     *         new topic ended up with (KIP-525)
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
                    $topicResult              = $response->topics[$newTopic->topic] ?? null;
                    $result[$newTopic->topic] = CreatedTopic::fromResponseTopic(
                        $newTopic->topic,
                        $topicResult,
                        self::topicError(
                            $newTopic->topic,
                            $topicResult?->errorCode,
                            $topicResult?->errorMessage
                        )
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
     * Asks the controller to raise the partition count of the given topics (ApiKey 37, Kafka 1.0, KIP-195)
     *
     * The api of KIP-195 is the last piece of `kafka-topics.sh --alter` that needed ZooKeeper. Like CreateTopics it
     * is served by the ACTIVE CONTROLLER alone - `$controller` has to be the node that
     * {@see \Protocol\Kafka\Admin\AdminClient::findController()} returned, and a broker that is not (or is no longer)
     * the controller reports the error code 41 (NotController) for every topic of the request, which is handed back
     * as a {@see Common\Errors\NotControllerException} of that topic instead of being thrown.
     *
     * Every entry of `$newPartitions` names the number of partitions its topic should have AFTERWARDS, as a
     * {@see NewPartitions} or as a plain integer; the api can only grow a topic, and a count that is not above the
     * current one is answered with 37 (InvalidPartitions).
     *
     * `$timeoutMs` is the time the controller waits for the new partitions to exist before it answers, as in
     * {@see self::createTopics()}: a value of 0 answers immediately with the error code 7 (RequestTimedOut) for
     * every accepted topic while the work carries on.
     *
     * @param Node                             $controller    Active controller of the cluster
     * @param array<string, NewPartitions|int> $newPartitions Topics to grow, as topic name => new total count
     * @param int                              $timeoutMs     How long the controller waits for the new partitions
     * @param bool                             $validateOnly  Validate the request without adding anything
     *
     * @return array<string, KafkaException|null> Error of every requested topic, null when it was grown
     */
    public function createPartitions(
        Node $controller,
        array $newPartitions,
        int $timeoutMs = 30000,
        bool $validateOnly = false
    ): array {
        $clientId = (string) $this->configuration[ClientConfig::CLIENT_ID];
        $topics   = array_map(strval(...), array_keys($newPartitions));

        return $this->controllerRequest(
            $controller,
            fn(int $correlationId): AbstractRequest => new CreatePartitionsRequest(
                $newPartitions,
                $timeoutMs,
                $validateOnly,
                $clientId,
                $correlationId
            ),
            CreatePartitionsResponse::class,
            static function (CreatePartitionsResponse $response) use ($topics): array {
                $result = [];
                foreach ($topics as $topic) {
                    $topicResult    = $response->topics[$topic] ?? null;
                    $result[$topic] = self::topicError(
                        $topic,
                        $topicResult?->errorCode,
                        $topicResult?->errorMessage
                    );
                }

                return $result;
            }
        );
    }

    /**
     * Asks the controller to elect the leader of the given partitions (ApiKey 43, Kafka 2.2, KIP-183/KIP-460)
     *
     * `$electionType` is the `election_type` byte Kafka 2.4 added with the **version 1** of the api (KIP-460):
     * {@see Admin\ElectionType::PREFERRED} moves the leadership back to the first replica of the assignment, and
     * {@see Admin\ElectionType::UNCLEAN} makes the first LIVE replica the leader even when none of them is in
     * sync. `$topicPartitions` is a `topic => list of partition ids` map, or **null** for every partition of the
     * cluster, and the answer is read into a `topic => partition => error` map with `null` for every partition
     * that really got a new leader.
     *
     * A partition that the controller left out of its answer - which happens for a **null** request, where every
     * partition that needed no election is dropped - is not in the result either: the caller asked for "whatever
     * needs electing", and nothing else is reported. The **top-level** error code of the version 1 answer is the
     * one case this method throws for: it is the 31 of a client the authorizer refused, which names no partition
     * at all.
     *
     * @param Node                          $controller      Active controller of the cluster
     * @param array<string, list<int>>|null $topicPartitions Partitions to elect a leader for, null for all of them
     * @param int                           $timeoutMs       How long the controller waits for the elections
     * @param int                           $electionType    Kind of election, an {@see Admin\ElectionType} constant
     *
     * @throws KafkaException If the answer carries a top-level error code (version 1 and above)
     *
     * @return array<string, array<int, KafkaException|null>> Error of every answered partition
     */
    public function electLeaders(
        Node $controller,
        ?array $topicPartitions,
        int $timeoutMs = ElectLeadersRequest::DEFAULT_TIMEOUT_MS,
        int $electionType = ElectionType::PREFERRED
    ): array {
        $clientId = (string) $this->configuration[ClientConfig::CLIENT_ID];

        return $this->controllerRequest(
            $controller,
            fn(int $correlationId): AbstractRequest => new ElectLeadersRequest(
                $topicPartitions,
                $timeoutMs,
                $electionType,
                $clientId,
                $correlationId
            ),
            ElectLeadersResponse::class,
            static function (ElectLeadersResponse $response): array {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['error' => 'The request as a whole was refused by the broker']
                    );
                }

                $result = [];
                foreach ($response->replicaElectionResults as $topic => $election) {
                    foreach ($election->partitionResult as $partition) {
                        $result[$topic][$partition->partitionId] = self::topicError(
                            $topic,
                            $partition->errorCode,
                            $partition->errorMessage
                        );
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Asks the controller to move the replicas of partitions to other brokers (ApiKey 45, Kafka 2.4, KIP-455)
     *
     * The api that took the last piece of `kafka-reassign-partitions.sh` away from ZooKeeper: before Kafka 2.4 a
     * reassignment was a JSON document written into the `/admin/reassign_partitions` znode, one at a time for the
     * whole cluster and impossible to cancel. Like CreateTopics it is served by the **active controller** alone -
     * `$controller` has to be the node that {@see \Protocol\Kafka\Admin\AdminClient::findController()} returned -
     * and a broker that is not (or is no longer) the controller answers the TOP-LEVEL error code 41
     * (NotController), which is thrown here, because it says nothing about the individual partitions.
     *
     * Every entry of `$reassignments` names the **whole** replica set its partition should end up with, as a
     * {@see NewPartitionReassignment} or as a plain list of broker ids - the first one is the preferred leader -
     * and `null` **cancels** a reassignment that is still in progress. The answer is one error per requested
     * partition, and a partition with the error code 0 is one the controller *accepted*: the data is moved
     * afterwards by the replica fetchers and watched with {@see self::listPartitionReassignments()}.
     *
     * The codes a 2.8.2 broker answers per partition are 3 (UnknownTopicOrPartition) for a topic or a partition it
     * does not have, 39 (InvalidReplicaAssignment) for an empty replica list or a broker that is not alive, and 85
     * (NoReassignmentInProgress) for a cancellation that had nothing to cancel.
     *
     * @param Node                                                              $controller    Active controller
     * @param array<string, array<int, list<int>|NewPartitionReassignment|null>> $reassignments Target replica set of
     *        every partition, as `topic => [partition => [broker ids]]`; `null` cancels that partition
     * @param int                                                               $timeoutMs     How long the
     *        controller waits for the reassignment to be registered
     *
     * @throws KafkaException If the request as a whole was refused, e.g. with 41 (NotController)
     *
     * @return array<string, array<int, KafkaException|null>> Error of every requested partition, null when accepted
     */
    public function alterPartitionReassignments(
        Node $controller,
        array $reassignments,
        int $timeoutMs = AlterPartitionReassignmentsRequest::DEFAULT_TIMEOUT_MS
    ): array {
        $clientId = (string) $this->configuration[ClientConfig::CLIENT_ID];

        return $this->controllerRequest(
            $controller,
            fn(int $correlationId): AbstractRequest => new AlterPartitionReassignmentsRequest(
                $reassignments,
                $timeoutMs,
                $clientId,
                $correlationId
            ),
            AlterPartitionReassignmentsResponse::class,
            static function (AlterPartitionReassignmentsResponse $response) use ($reassignments): array {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['error' => $response->errorMessage ?? 'The controller refused the whole request']
                    );
                }

                $result = [];
                foreach ($reassignments as $topic => $partitions) {
                    foreach (array_keys($partitions) as $partition) {
                        $answer = $response->responses[$topic]->partitions[$partition] ?? null;
                        $result[(string) $topic][(int) $partition] = self::partitionError(
                            (string) $topic,
                            (int) $partition,
                            $answer?->errorCode,
                            $answer?->errorMessage
                        );
                    }
                }

                return $result;
            }
        );
    }

    /**
     * Asks the controller which partitions are being reassigned right now (ApiKey 46, Kafka 2.4, KIP-455)
     *
     * The other half of KIP-455, and the replacement of `kafka-reassign-partitions.sh --verify`: a partition is in
     * the answer while its reassignment is in flight and disappears from it when the controller is done. A topic or
     * a partition that does not exist is not an error here - it is simply absent, because the answer is what is
     * going on and not what was asked for - and the only error code is the top-level one, which is thrown.
     *
     * **`null` asks for every reassignment of the cluster**, an empty array for none of them; on a shared cluster a
     * caller should name its own partitions.
     *
     * @param Node                          $controller Active controller of the cluster
     * @param array<string, list<int>>|null $partitions Partitions to ask for, `null` for the whole cluster
     * @param int                           $timeoutMs  How long the controller waits before it answers
     *
     * @throws KafkaException If the request was refused, e.g. with 41 (NotController)
     *
     * @return list<PartitionReassignment> Every partition that is being reassigned, in the order of the answer
     */
    public function listPartitionReassignments(
        Node $controller,
        ?array $partitions = null,
        int $timeoutMs = ListPartitionReassignmentsRequest::DEFAULT_TIMEOUT_MS
    ): array {
        $clientId = (string) $this->configuration[ClientConfig::CLIENT_ID];

        return $this->controllerRequest(
            $controller,
            fn(int $correlationId): AbstractRequest => new ListPartitionReassignmentsRequest(
                $partitions,
                $timeoutMs,
                $clientId,
                $correlationId
            ),
            ListPartitionReassignmentsResponse::class,
            static function (ListPartitionReassignmentsResponse $response): array {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['error' => $response->errorMessage ?? 'The controller refused the request']
                    );
                }

                $reassignments = [];
                foreach ($response->topics as $topic => $topicReassignment) {
                    foreach ($topicReassignment->partitions as $partition) {
                        $reassignments[] = new PartitionReassignment(
                            (string) $topic,
                            $partition->partitionIndex,
                            $partition->replicas,
                            $partition->addingReplicas,
                            $partition->removingReplicas
                        );
                    }
                }

                return $reassignments;
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
     * Turns the error code of one partition of a reassignment answer into the exception of the caller
     *
     * A partition the controller did not report on at all is an answer this client can not interpret, so it becomes
     * an {@see UnknownErrorException} instead of a silent success.
     */
    private static function partitionError(
        string $topic,
        int $partition,
        ?int $errorCode,
        ?string $errorMessage = null
    ): ?KafkaException {
        if ($errorCode === null) {
            return new UnknownErrorException([
                'topic'     => $topic,
                'partition' => $partition,
                'error'     => 'The controller sent no result for this partition',
            ]);
        }
        if ($errorCode === KafkaException::NO_ERROR) {
            return null;
        }

        $context = ['topic' => $topic, 'partition' => $partition];
        if ($errorMessage !== null && $errorMessage !== '') {
            $context['error'] = $errorMessage;
        }

        return KafkaException::fromCode($errorCode, $context);
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

    /**
     * Enrols topic-partitions into the open transaction of a producer (ApiKey 24, Kafka 0.11, KIP-98)
     *
     * The request goes to the **transaction coordinator** of the transactional id, and it has to be answered before
     * the first Produce request that writes into one of those partitions: the coordinator keeps the list of
     * partitions of a transaction and writes a control batch into every one of them when the transaction ends, so
     * a partition that was never added would keep its records uncommitted forever. The first call of a transaction
     * is also the one that *starts* it on the broker - the protocol has no "BeginTransaction" request.
     *
     * There is no top-level error code in the answer: a failure of the transaction itself - 47 for a fenced epoch,
     * 48 for a state that may not add partitions, 49 for a producer id the coordinator does not hold, 51 while the
     * previous transaction is still being completed - is repeated on every partition, so the first error code of
     * the answer is the one that is reported here.
     *
     * @param Node               $coordinatorNode    Transaction coordinator of the transactional id
     * @param string             $transactionalId    `transactional.id` of the producer
     * @param ProducerIdAndEpoch $producerIdAndEpoch Producer id and epoch of the open transaction
     * @param array<string, list<int>> $topicPartitions Partitions to add, as topic => list of partition ids
     *
     * @throws KafkaException The error code of the first partition that was refused
     */
    public function addPartitionsToTxn(
        Node $coordinatorNode,
        string $transactionalId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        array $topicPartitions
    ): void {
        $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): AddPartitionsToTxnRequest => new AddPartitionsToTxnRequest(
                $transactionalId,
                $producerIdAndEpoch->producerId,
                $producerIdAndEpoch->epoch,
                $topicPartitions,
                $this->configuration[ClientConfig::CLIENT_ID],
                $correlationId
            ),
            AddPartitionsToTxnResponse::class,
            static function (AddPartitionsToTxnResponse $response) use ($transactionalId): void {
                foreach ($response->errors as $topic => $topicErrors) {
                    /** @var AddPartitionsToTxnResponsePartition $partitionError */
                    foreach ($topicErrors->partitionErrors as $partitionId => $partitionError) {
                        if ($partitionError->errorCode !== KafkaException::NO_ERROR) {
                            throw KafkaException::fromCode($partitionError->errorCode, [
                                'transactionalId' => $transactionalId,
                                'topic'           => $topic,
                                'partitionId'     => $partitionId,
                            ]);
                        }
                    }
                }
            }
        );
    }

    /**
     * Enrols the offsets of a consumer group into the open transaction (ApiKey 25, Kafka 0.11, KIP-98)
     *
     * The first half of `sendOffsetsToTransaction()`: the request goes to the **transaction coordinator** and puts
     * the partition of `__consumer_offsets` that the group hashes to on the list of partitions of the transaction,
     * so that the commit marker reaches it as well. The offsets themselves travel in the
     * {@see self::txnOffsetCommit()} that has to follow it, and that goes to the group coordinator instead.
     *
     * @param Node               $coordinatorNode    Transaction coordinator of the transactional id
     * @param string             $transactionalId    `transactional.id` of the producer
     * @param ProducerIdAndEpoch $producerIdAndEpoch Producer id and epoch of the open transaction
     * @param string             $groupId            Consumer group whose offsets become part of the transaction
     *
     * @throws KafkaException The error code of the answer
     */
    public function addOffsetsToTxn(
        Node $coordinatorNode,
        string $transactionalId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        string $groupId
    ): void {
        $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): AddOffsetsToTxnRequest => new AddOffsetsToTxnRequest(
                $transactionalId,
                $producerIdAndEpoch->producerId,
                $producerIdAndEpoch->epoch,
                $groupId,
                $this->configuration[ClientConfig::CLIENT_ID],
                $correlationId
            ),
            AddOffsetsToTxnResponse::class,
            static function (AddOffsetsToTxnResponse $response) use ($transactionalId, $groupId): void {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $response->errorCode,
                        ['transactionalId' => $transactionalId, 'groupId' => $groupId]
                    );
                }
            }
        );
    }

    /**
     * Commits or aborts the open transaction of a producer (ApiKey 26, Kafka 0.11, KIP-98)
     *
     * The last request of a transaction, sent to the **transaction coordinator**. The answer means that the
     * coordinator has *decided* the outcome and written it into `__transaction_state`, not that the control batches
     * are in the partitions: those are written afterwards, with a `WriteTxnMarkers` request per partition leader, so
     * a `read_committed` consumer sees the records of a committed transaction a moment after this call returns.
     *
     * @param Node               $coordinatorNode    Transaction coordinator of the transactional id
     * @param string             $transactionalId    `transactional.id` of the producer
     * @param ProducerIdAndEpoch $producerIdAndEpoch Producer id and epoch of the open transaction
     * @param bool               $transactionResult  {@see EndTxnRequest::COMMIT} or {@see EndTxnRequest::ABORT}
     *
     * @throws KafkaException The error code of the answer
     */
    public function endTxn(
        Node $coordinatorNode,
        string $transactionalId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        bool $transactionResult
    ): void {
        $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): EndTxnRequest => new EndTxnRequest(
                $transactionalId,
                $producerIdAndEpoch->producerId,
                $producerIdAndEpoch->epoch,
                $transactionResult,
                $this->configuration[ClientConfig::CLIENT_ID],
                $correlationId
            ),
            EndTxnResponse::class,
            static function (EndTxnResponse $response) use ($transactionalId, $transactionResult): void {
                if ($response->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode($response->errorCode, [
                        'transactionalId'   => $transactionalId,
                        'transactionResult' => $transactionResult ? 'commit' : 'abort',
                    ]);
                }
            }
        );
    }

    /**
     * Commits consumer offsets inside the open transaction (ApiKey 28, Kafka 0.11, KIP-98)
     *
     * The second half of `sendOffsetsToTransaction()` and the only request of the transaction protocol that goes to
     * the **group coordinator** ({@see self::getGroupCoordinator()}), because that is the broker which owns
     * `__consumer_offsets`. It has to follow an {@see self::addOffsetsToTxn()} for the same group, otherwise the
     * group coordinator answers 48 (`InvalidTxnState`) - a transactional write into a partition that is not part of
     * an open transaction.
     *
     * The offsets it writes stay invisible to an OffsetFetch of the group until the transaction is committed; an
     * aborted transaction leaves the group with the offsets it had before. As in
     * {@see self::addPartitionsToTxn()} there is no top-level error code, so the first error code of the answer is
     * the one that is reported here.
     *
     * @param Node               $coordinatorNode    **Group** coordinator of `$groupId`
     * @param string             $transactionalId    `transactional.id` of the producer
     * @param string             $groupId            Consumer group whose offsets are committed
     * @param ProducerIdAndEpoch $producerIdAndEpoch Producer id and epoch of the open transaction
     * @param array<string, array<int, int|OffsetAndMetadata>> $topicPartitionOffsets Offsets to commit
     *
     * @throws KafkaException The error code of the first partition that was refused
     */
    public function txnOffsetCommit(
        Node $coordinatorNode,
        string $transactionalId,
        string $groupId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        array $topicPartitionOffsets
    ): void {
        $this->coordinatorRequest(
            $coordinatorNode,
            fn(int $correlationId): TxnOffsetCommitRequest => new TxnOffsetCommitRequest(
                $transactionalId,
                $groupId,
                $producerIdAndEpoch->producerId,
                $producerIdAndEpoch->epoch,
                $topicPartitionOffsets,
                $this->configuration[ClientConfig::CLIENT_ID],
                $correlationId
            ),
            TxnOffsetCommitResponse::class,
            static function (TxnOffsetCommitResponse $response) use ($transactionalId, $groupId): void {
                foreach ($response->topics as $topic => $topicResult) {
                    /** @var TxnOffsetCommitResponsePartition $partitionResult */
                    foreach ($topicResult->partitions as $partitionId => $partitionResult) {
                        if ($partitionResult->errorCode !== KafkaException::NO_ERROR) {
                            throw KafkaException::fromCode($partitionResult->errorCode, [
                                'transactionalId' => $transactionalId,
                                'groupId'         => $groupId,
                                'topic'           => $topic,
                                'partitionId'     => $partitionId,
                            ]);
                        }
                    }
                }
            }
        );
    }
}
