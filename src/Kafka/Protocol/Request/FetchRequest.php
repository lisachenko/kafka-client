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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopicV7;
use Protocol\Kafka\Protocol\Data\FetchRequestReplicaState;
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV0;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV12;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV5;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV9;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * Fetch API (key 1), version 15
 *
 * The fetch API is used to fetch a chunk of one or more logs for some topic-partitions. Logically one specifies the
 * topics, partitions, and starting offset at which to begin the fetch and gets back a chunk of messages. In general,
 * the return messages will have offsets larger than or equal to the starting offset. However, with compressed
 * messages, it's possible for the returned messages to have offsets smaller than the starting offset. The number of
 * such messages is typically small and the caller is responsible for filtering out those messages.
 *
 * Fetch requests follow a long poll model so they can be made to block for a period of time if sufficient data is not
 * immediately available.
 *
 * As an optimization the server is allowed to return a partial message at the end of the message set. Clients should
 * handle this case.
 *
 * <pre>
 *   FetchRequest (Version: 15) => MaxWaitTime MinBytes MaxBytes IsolationLevel SessionId Epoch
 *                                 [TopicId [Partition CurrentLeaderEpoch FetchOffset LastFetchedEpoch
 *                                           LogStartOffset MaxBytes TAG_BUFFER] TAG_BUFFER]
 *                                 [TopicId [Partition] TAG_BUFFER] RackId TAG_BUFFER
 *     ReplicaId      => int32     -- versions 0 to 14 only, replaced by the tagged ReplicaState
 *     ReplicaState   => tag 1, [ReplicaId int32 ReplicaEpoch int64] -- since version 15
 *     MaxWaitTime    => int32
 *     MinBytes       => int32
 *     MaxBytes       => int32
 *     IsolationLevel => int8
 *     SessionId      => int32     -- since version 7
 *     Epoch          => int32     -- since version 7
 *     CurrentLeaderEpoch => int32 -- since version 9
 *     FetchOffset    => int64
 *     LastFetchedEpoch => int32   -- since version 12
 *     LogStartOffset => int64
 *     RackId         => compact string -- since version 11, compact since version 12
 *     ClusterId      => tag 0, compact nullable string -- since version 12
 *     TopicName      => compact string -- versions 0 to 12 only
 *     TopicId        => uuid, 16 raw bytes -- since version 13
 * </pre>
 *
 * Every `TopicName` and `RackId` of a version 12 frame is a COMPACT string, every array a compact one, and the
 * `TAG_BUFFER` of the grammar is the tagged-field section that closes every structure of a flexible version.
 *
 * The eleven versions of this api that a 2.8.2 broker serves below this one differ as follows:
 *
 * * **v1** (Kafka 0.9) left the request untouched and only prefixed the answer with `ThrottleTimeMs`, see
 *   {@see FetchResponse};
 * * **v2** (Kafka 0.10.0) is byte-identical to v1 in both directions - `FETCH_REQUEST_V2` is `FETCH_REQUEST_V1` in
 *   `FetchRequest.schemaVersions()` @ 1.1.1 - and means one thing only: *the client understands message format v1*.
 *   A broker answers a request below version 2 by converting every stored record down to message format v0
 *   (`KafkaApis.handleFetchRequest`), which strips the timestamps and turns the relative inner offsets of a
 *   compressed set back into absolute ones;
 * * **v3** (Kafka 0.10.1, KIP-74) adds the request-level `MaxBytes` after `MinBytes`, which bounds the **whole**
 *   answer instead of a single partition, see {@see self::$maxBytes};
 * * **v4** (Kafka 0.11.0, KIP-98) adds the `IsolationLevel` after it and is the first version that is answered
 *   with the log as it lies: a record batch of the message format v2, with the headers, the producer ids and the
 *   transaction flags that no lower version has a place for. Its answer carries the `LastStableOffset` and the
 *   `AbortedTransactions` of every partition;
 * * **v5** (KIP-107) adds the `LogStartOffset` of a partition entry, which only a follower fills in, and the
 *   `LogStartOffset` of every partition of the answer;
 * * **v6** (Kafka 1.0) is byte-identical to v5 in both directions and says that the client understands the error
 *   code **56** `KAFKA_STORAGE_ERROR`, which a broker translates to 6 `NOT_LEADER_FOR_PARTITION` for a request of
 *   version 5 or lower ({@see FetchRequestV6});
 * * **v7** (Kafka 1.1, KIP-227) adds the **incremental fetch sessions**: the `SessionId` and the `Epoch` of
 *   {@see FetchMetadata} in front of the topics array, and the trailing `forgotten_topics_data` behind it
 *   ({@see \Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopic}); the answer gains a top-level error code
 *   and the session id, see {@see FetchResponse};
 * * **v8** (Kafka 2.0, KIP-219) is byte-identical to v7 in both directions - `FetchRequest.json` @ 2.8.2 has no
 *   field of it and its only comment is "Version 8 is the same as version 7" - and states that the **client**
 *   honours `throttle_time_ms` itself: a throttled fetch is answered *before* the delay, with an empty topics
 *   array, and the channel is muted for the reported time afterwards. This class is that version, and
 *   {@see \Protocol\Kafka\Client} sleeps the remaining throttle time before its next request to that broker
 *   unless {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT} switches it off. A 2.8.2 broker throttles
 *   a version 7 fetch ({@see FetchRequestV7}) in exactly the same way - the version is the promise of the client,
 *   not a switch of the broker;
 * * **v9** (Kafka 2.1, KIP-320) adds `current_leader_epoch` to every partition entry of the request, between the
 *   partition index and the fetch offset, see
 *   {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::$currentLeaderEpoch}: the epoch the client
 *   believes the partition is being led with, which fences a consumer whose metadata is out of date with **74**
 *   `FENCED_LEADER_EPOCH` or **75** `UNKNOWN_LEADER_EPOCH` ({@see FetchRequestV9} keeps that version);
 * * **v10** (Kafka 2.1, KIP-110) is byte-identical to v9 in both directions - `FetchRequest.json` @ 2.8.2 has no
 *   field of it - and states that the client understands a **zstd**-compressed record batch. A fetch below this
 *   version of a partition whose records are zstd-compressed is refused with **76**
 *   `UNSUPPORTED_COMPRESSION_TYPE`, because the broker does not down-convert zstd;
 * * **v11** (Kafka 2.3, KIP-392) appends the `rack_id` of the consumer to the request - the last field of the
 *   frame, behind the forgotten topics - and the `preferred_read_replica` to every partition entry of the answer,
 *   see {@see self::$rackId} and {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition::$preferredReadReplica}
 *   ({@see FetchRequestV11} keeps that version);
 * * **v12** (Kafka 2.7) is the first **flexible** version of the api (KIP-482), see {@see self::FLEXIBLE_VERSION},
 *   and adds two things of KIP-595 on top of the encoding: the `last_fetched_epoch` of every partition entry,
 *   which lets the **leader** detect a divergence of the logs in the fetch itself instead of in a separate
 *   OffsetForLeaderEpoch round trip (see
 *   {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::$lastFetchedEpoch} and the tagged
 *   `diverging_epoch` of the answer), and the tagged `cluster_id` of the request ({@see FetchRequestV12} keeps
 *   that version);
 * * **v13** (Kafka 3.1, KIP-516) **replaces the topic names of the frame with topic ids**: `FetchRequest.json`
 *   @ 3.1.2 declares the `Topic` of a topic entry and of a forgotten-topic entry as `versions 0-12` and their
 *   `TopicId` as `13+`, so a request names every topic by the 16 raw bytes the controller gave it and carries no
 *   name at all. A client therefore has to know the id of every topic it fetches - {@see self::$topicIds} is
 *   where it states them, {@see \Protocol\Kafka\Common\Cluster::topicIdOf()} where it learns them - and a
 *   topic whose id it does not know is fetched after a metadata refresh, never by name. An id the broker does not
 *   host is answered with **100** `UnknownTopicId` per **partition**, and a session whose earlier requests named
 *   the topics the other way round with the top-level **106** `FetchSessionTopicIdError` that the same release
 *   added ({@see FetchRequestV13} keeps that version);
 * * **v14** (Kafka 3.5, KIP-405) is byte-identical to v13 in both directions - `FetchRequest.json` @ 3.5.2 has no
 *   field of it and its whole comment is "Version 14 is the same as version 13 but it also receives a new error
 *   called OffsetMovedToTieredStorageException" - and states that the client understands the error code **109**
 *   `OFFSET_MOVED_TO_TIERED_STORAGE`, which a broker with remote storage answers for a fetch offset that is no
 *   longer on its local disk. A version below 14 gets **1** `OffsetOutOfRange` for the same condition
 *   (`ReplicaManager.handleOffsetMovedToTieredStorage` @ 3.9.2); this node has no remote storage configured, so
 *   the code cannot be produced on it and the version is documented on the wire alone
 *   ({@see FetchRequestV14} keeps it);
 * * **v15** (Kafka 3.5, KIP-903) **deprecates the top-level `replica_id`** - it is `versions 0-14` from that
 *   release on - and puts a tagged `replica_state` of a replica id **and a replica epoch** in its place, see
 *   {@see self::$replicaState} and {@see \Protocol\Kafka\Protocol\Data\FetchRequestReplicaState}. A consumer's
 *   state is the default `-1` / `-1`, which a tagged field does not write at all, so a version 15 consumer fetch
 *   is the version 14 frame **minus** the four bytes of the old field. This class is that version.
 *
 * A request of version 7 and above **without** a session - the `session_id 0` / `epoch -1` of {@see FetchMetadata::legacy()},
 * which is what this class sends when it is given no metadata - is served exactly like a version 6 request: the
 * whole requested set comes back and the answer reports `session_id = 0`. That is what
 * {@see \Protocol\Kafka\Client::fetchPartitions()} sends today.
 *
 * {@see FetchRequestV14}, {@see FetchRequestV13}, {@see FetchRequestV12}, {@see FetchRequestV11},
 * {@see FetchRequestV10}, {@see FetchRequestV9}, {@see FetchRequestV8},
 * {@see FetchRequestV7}, {@see FetchRequestV6}, {@see FetchRequestV5}, {@see FetchRequestV4},
 * {@see FetchRequestV3}, {@see FetchRequestV2}, {@see FetchRequestV1} and {@see FetchRequestV0} keep the lower
 * versions available.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v15)", "Fetch sessions (v7, KIP-227)",
 *      "The topic ids of the fetch path (v13, KIP-516)" and "The replica state of KIP-903 (v15)"
 */
class FetchRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::FETCH;

    /**
     * @inheritdoc
     */
    public const int VERSION = 15;

    /**
     * First version of this api whose frame is written with the compact types and the tagged fields of KIP-482
     *
     * `FetchRequest.json` @ 3.1.2 declares `"flexibleVersions": "12+"`: a version 12 request carries the request
     * header **v2**, every string and array as a compact one, and a tagged-field section at the end of the body,
     * of every topic entry and of every partition entry. The `cluster_id` of the same version is itself a
     * **tagged** field (tag 0), see {@see self::$clusterId}.
     */
    public const int FLEXIBLE_VERSION = 12;

    /**
     * Value of the `rack_id` of version 11 that names no rack: the empty string (KIP-392)
     */
    public const string NO_RACK = '';

    /**
     * Default bound of a whole answer, the 50 MiB of the `fetch.max.bytes` option of the Java consumer
     *
     * @see \Protocol\Kafka\Consumer\ConsumerConfig::FETCH_MAX_BYTES
     */
    public const int DEFAULT_MAX_BYTES = 52428800;

    /**
     * `isolation_level` of version 4 and above (Kafka 0.11, KIP-98): every record of the log is visible, transactional
     * ones included, up to the high water mark. What the Offsets api v2 and `ConsumerConfig::ISOLATION_LEVEL` send
     * by default.
     */
    public const int READ_UNCOMMITTED = 0;

    /**
     * `isolation_level` of version 4 and above: only non-transactional and *committed* transactional records are
     * visible, and the fetch stops at the last stable offset instead of the high water mark.
     */
    public const int READ_COMMITTED = 1;

    /**
     * Topics to fetch from, indexed by the topic name below version 13 and a plain list from version 13 on
     *
     * The identity of a topic entry of version 13 is its **id**, 16 raw bytes that no json vector and no error
     * message can carry as an array key, so a version 13 frame decodes into a list and the client resolves the
     * ids against the map it built the request from, see {@see self::$topicIds}.
     *
     * @var array<array-key, FetchRequestTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * Id of every topic of this request, as topic name => the 16 raw bytes of its `uuid` (KIP-516)
     *
     * A version 13 request writes these ids instead of the names and refuses to be built without one for every
     * topic it names: a fetch by name is exactly what KIP-516 took away, and the client learns the ids from a
     * Metadata answer, {@see \Protocol\Kafka\Common\Cluster::topicIdOf()}. Every version below 13 ignores the
     * map altogether.
     *
     * @since Version 13 of protocol (Kafka 3.1, KIP-516)
     *
     * @var array<string, string>
     */
    protected readonly array $topicIds;

    /**
     * Id of the fetch session this request belongs to, 0 for a request that has none.
     *
     * @since Version 7 of protocol
     */
    protected readonly int $sessionId;

    /**
     * Epoch of that fetch session: 0 asks for a new session, -1 says "no session", anything above 0 is an
     * incremental fetch of an existing one.
     *
     * @since Version 7 of protocol
     */
    protected readonly int $epoch;

    /**
     * Partitions the fetch session should drop, one entry per topic
     *
     * @since Version 7 of protocol
     *
     * @var list<FetchRequestForgottenTopic>
     */
    protected readonly array $forgottenTopics;

    /**
     * Cluster this request is meant for, the tagged `cluster_id` of version 12 (Kafka 2.7).
     *
     * `null` - the default of the specification and of this client - leaves the field off the wire altogether,
     * which is what a tagged field whose value is its default does. It exists for the raft replication of
     * KIP-595, where a broker that has not registered yet validates that it is talking to the cluster it thinks
     * it is; a consumer has nothing to say here, and a broker that is given a wrong one answers **100**
     * `INCONSISTENT_CLUSTER_ID`.
     *
     * The property is declared here instead of being promoted in the constructor, because a **tagged** field is
     * left out of a frame that does not carry it: an unpacked request would leave a promoted property
     * uninitialized, and writing it back out again would fail.
     *
     * @since Version 12 of protocol
     */
    protected ?string $clusterId = null;

    /**
     * Who this fetch comes from, the tagged `replica_state` of version 15 (Kafka 3.5, KIP-903)
     *
     * `null` - what a consumer means and what this client sends - is the pair `-1` / `-1` of the specification and
     * leaves the field off the wire altogether, which is what a tagged field whose value is its default does. A
     * caller that really is a follower states its node id and its **broker epoch** here, and the leader of the
     * partition fences a fetch whose epoch is older than the one the controller published for that broker
     * ({@see \Protocol\Kafka\Protocol\Data\FetchRequestReplicaState}).
     *
     * Every version below 15 carries the node id in the plain {@see self::$replicaId} field instead and has no
     * place for an epoch at all; the property is declared here rather than promoted in the constructor for the
     * reason {@see self::$clusterId} is - a tagged field that a frame does not carry would leave a promoted
     * property uninitialized.
     *
     * @since Version 15 of protocol (Kafka 3.5, KIP-903)
     */
    protected ?FetchRequestReplicaState $replicaState = null;

    /**
     * @param array<string, array<int, int|array{int, int}>> $topicPartitions Fetch offset of every partition, as
     *                                                          topic => partition => offset. The **order** of this
     *                                                          array is the order the broker fills the answer in,
     *                                                          see {@see self::$maxBytes}.
     *
     *                                                          A value may also be the pair
     *                                                          `[offset, currentLeaderEpoch]`, which is how a
     *                                                          caller states the leader epoch that version 9
     *                                                          (KIP-320) puts on the wire, or the triple
     *                                                          `[offset, currentLeaderEpoch, lastFetchedEpoch]`,
     *                                                          which adds the epoch of the last record it really
     *                                                          read (version 12, KIP-595); a plain integer is the
     *                                                          offset with
     *                                                          {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::UNKNOWN_LEADER_EPOCH},
     *                                                          which is what every call written before Kafka 2.1
     *                                                          means and what a version below 9 sends anyway.
     * @param int                            $maxWaitTime       The maximum amount of time in milliseconds to block
     *                                                          waiting if insufficient data is available at the time
     *                                                          the request is issued.
     * @param int                            $minBytes          The minimum number of bytes of messages that must be
     *                                                          available to give a response. With 0 the server
     *                                                          always responds immediately, with 1 as soon as at
     *                                                          least one partition has at least one byte of data, or
     *                                                          when $maxWaitTime is over.
     * @param int                            $partitionMaxBytes The maximum number of bytes to include in the message
     *                                                          set of one partition, the `MaxBytes` of every
     *                                                          partition entry (`max.partition.fetch.bytes`).
     * @param int                            $replicaId         The node id of the replica that initiates this
     *                                                          request. Ordinary consumers always send -1 as they
     *                                                          have no node id; -2 is accepted from a non-broker
     *                                                          that wants to fetch as if it were a replica, for
     *                                                          debugging purposes. **From version 15 on the value
     *                                                          travels in the tagged `replica_state`** instead of
     *                                                          in a field of its own (KIP-903), see
     *                                                          {@see self::$replicaState}, and a -1 leaves it off
     *                                                          the wire.
     * @param int                            $maxBytes          The maximum number of bytes of the whole answer
     *                                                          (`fetch.max.bytes`), the field that version 3 added.
     * @param FetchMetadata|null             $metadata          Session id and epoch of version 7, `null` for the
     *                                                          session-less full fetch that every version below 7
     *                                                          sends, {@see FetchMetadata::legacy()}.
     * @param array<string, list<int>>       $forgottenTopicPartitions Partitions the session should forget, as
     *                                                          topic => [partition, ...]; only version 7 puts them
     *                                                          on the wire and only a session does anything with
     *                                                          them.
     * @param string|null                    $clusterId        The tagged `cluster_id` of version 12,
     *                                                          {@see self::$clusterId}; `null` leaves it off the
     *                                                          wire, and that is what a client sends.
     * @param array<string, string>          $topicIds          Id of every topic named above, as name => the 16
     *                                                          raw bytes of its uuid; **version 13 needs one per
     *                                                          topic** and throws {@see UnknownTopicIdException}
     *                                                          without it, every lower version ignores the map.
     * @param int|null                       $replicaEpoch      Broker epoch of the follower named in `$replicaId`,
     *                                                          the second half of the `replica_state` of version 15
     *                                                          (KIP-903); `null` and `-1` are "unknown", and a
     *                                                          consumer - `$replicaId = -1` - leaves the whole
     *                                                          structure off the wire.
     */
    public function __construct(
        array $topicPartitions,
        protected readonly int $maxWaitTime,
        protected readonly int $minBytes,
        int $partitionMaxBytes,
        protected readonly int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Maximum number of bytes the broker may put into the whole answer, since version 3 (KIP-74).
         *
         * The broker fills the partitions **in the order of the request** and stops once this budget is used up
         * (`ReplicaManager.readFromLocalLog` @ 0.10.2.2 subtracts the size of every partition it read from
         * `limitBytes`), so the partitions at the end of a request come back empty while the ones in front of them
         * carry data. A client that fetches more than one partition therefore has to rotate their order between
         * requests, otherwise the last ones would never be served.
         *
         * It is **not** a hard limit: as long as nothing has been read yet, the first non-empty partition ignores
         * both this bound and its own `MaxBytes` and returns at least one complete message, so a consumer always
         * makes progress. The other side of that coin is that an answer may exceed this value.
         */
        protected readonly int $maxBytes = self::DEFAULT_MAX_BYTES,
        /**
         * Visibility of transactional records, since version 4 (KIP-98).
         *
         * {@see self::READ_UNCOMMITTED} shows every record of the log up to the high water mark, transactional
         * ones included and whether or not their transaction was ever committed; {@see self::READ_COMMITTED} stops
         * the answer at the `LastStableOffset` of the partition and makes the broker report the transactions it
         * aborted in that range, so that the consumer can drop their records. A version below 4 does not carry the
         * field at all and always reads uncommitted.
         */
        protected readonly int $isolationLevel = self::READ_UNCOMMITTED,
        ?FetchMetadata $metadata = null,
        array $forgottenTopicPartitions = [],
        /**
         * Rack of the consumer that sends this request, since version 11 (Kafka 2.3, KIP-392).
         *
         * The leader of a partition reads it to pick the replica the consumer should read from - its
         * `replica.selector.class` maps a rack to a replica - and answers that node id in the
         * `preferred_read_replica` of every partition entry, see
         * {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition::$preferredReadReplica}. The empty string
         * is "no rack", the default of the field and of
         * {@see \Protocol\Kafka\Consumer\ConsumerConfig::CLIENT_RACK}; a broker without a selector ignores the
         * field altogether.
         */
        protected readonly string $rackId = self::NO_RACK,
        ?string $clusterId = null,
        array $topicIds = [],
        ?int $replicaEpoch = null
    ) {
        $this->clusterId = $clusterId;
        $this->topicIds  = $topicIds;

        // The `replica_state` of version 15 is a TAGGED field whose default is the pair -1 / -1: a consumer says
        // nothing at all, and only a caller that really claims to be a replica puts the structure on the wire
        $replicaEpoch ??= FetchRequestReplicaState::UNKNOWN;
        if ($replicaId !== FetchRequestReplicaState::UNKNOWN || $replicaEpoch !== FetchRequestReplicaState::UNKNOWN) {
            $this->replicaState = new FetchRequestReplicaState($replicaId, $replicaEpoch);
        }

        $metadata ??= FetchMetadata::legacy();
        $this->sessionId = $metadata->sessionId;
        $this->epoch     = $metadata->epoch;

        $forgottenClass  = static::forgottenTopicClass();
        $forgottenTopics = [];
        foreach ($forgottenTopicPartitions as $topic => $partitions) {
            $forgottenTopics[] = new $forgottenClass(
                (string) $topic,
                array_values($partitions),
                self::idOf($topicIds, (string) $topic)
            );
        }
        $this->forgottenTopics = $forgottenTopics;

        $topicClass            = static::topicClass();
        $partitionClass        = $topicClass::partitionClass();
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitionOffsets) {
            $partitions = [];
            foreach ($partitionOffsets as $partition => $fetchOffset) {
                [$offset, $currentLeaderEpoch] = self::offsetAndEpoch($fetchOffset);
                $partitions[$partition]        = new $partitionClass(
                    $partition,
                    $offset,
                    $partitionMaxBytes,
                    FetchRequestTopicPartition::INVALID_LOG_START_OFFSET,
                    $currentLeaderEpoch,
                    self::lastFetchedEpochOf($fetchOffset)
                );
            }
            $entry = new $topicClass((string) $topic, $partitions, self::idOf($topicIds, (string) $topic));
            // A version 13 entry carries no name at all, so the list it travels in is the only honest shape
            if (static::VERSION >= 13) {
                $packedTopicPartitions[] = $entry;
            } else {
                $packedTopicPartitions[$topic] = $entry;
            }
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds a request from a list of topic partitions with the offset to start fetching each of them at
     *
     * The order of the pairs is kept, because it is the order in which the broker fills the answer of a version 3
     * request until its `MaxBytes` are used up.
     *
     * @param iterable<array{TopicPartition, int|array{int, int}}> $partitionOffsets Pairs of a topic partition and
     *                                                                       its fetch offset, the offset optionally
     *                                                                       being the `[offset, epoch]` pair of
     *                                                                       version 9
     * @param array<string, list<int>>             $forgottenTopicPartitions Partitions the session should forget
     * @param array<string, string>                $topicIds Id of every topic named above, see {@see self::$topicIds}
     */
    public static function fromTopicPartitions(
        iterable $partitionOffsets,
        int $maxWaitTime,
        int $minBytes,
        int $partitionMaxBytes,
        int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0,
        int $maxBytes = self::DEFAULT_MAX_BYTES,
        int $isolationLevel = self::READ_UNCOMMITTED,
        ?FetchMetadata $metadata = null,
        array $forgottenTopicPartitions = [],
        array $topicIds = []
    ): static {
        $topicPartitions = [];
        foreach ($partitionOffsets as [$topicPartition, $fetchOffset]) {
            $topicPartitions[$topicPartition->topic][$topicPartition->partition] = $fetchOffset;
        }

        return new static(
            $topicPartitions,
            $maxWaitTime,
            $minBytes,
            $partitionMaxBytes,
            $replicaId,
            $clientId,
            $correlationId,
            $maxBytes,
            $isolationLevel,
            $metadata,
            $forgottenTopicPartitions,
            self::NO_RACK,
            null,
            $topicIds
        );
    }

    /**
     * Splits a value of the `$topicPartitions` map into the fetch offset and the current leader epoch.
     *
     * A plain integer is the offset of a caller that does not track leader epochs, which is what every call of
     * this client meant before Kafka 2.1; the pair `[offset, epoch]` is how a caller states the epoch that
     * version 9 (KIP-320) puts on the wire.
     *
     * @param int|array{int, int} $fetchOffset
     *
     * @return array{int, int} the fetch offset and the current leader epoch
     */
    public static function offsetAndEpoch(int|array $fetchOffset): array
    {
        if (is_array($fetchOffset)) {
            return [(int) $fetchOffset[0], (int) $fetchOffset[1]];
        }

        return [$fetchOffset, FetchRequestTopicPartition::UNKNOWN_LEADER_EPOCH];
    }

    /**
     * Reads the `last_fetched_epoch` of version 12 out of a value of the `$topicPartitions` map.
     *
     * The third element of the triple `[offset, currentLeaderEpoch, lastFetchedEpoch]` is the epoch of the last
     * record the caller really read from the partition, the field version 12 (KIP-595) added; an offset and a
     * pair both mean {@see FetchRequestTopicPartition::UNKNOWN_LAST_FETCHED_EPOCH}, which is what the Java
     * consumer @ 2.8.2 sends for every partition of every fetch.
     *
     * @param int|array{int, int}|array{int, int, int} $fetchOffset
     */
    public static function lastFetchedEpochOf(int|array $fetchOffset): int
    {
        if (is_array($fetchOffset) && isset($fetchOffset[2])) {
            return (int) $fetchOffset[2];
        }

        return FetchRequestTopicPartition::UNKNOWN_LAST_FETCHED_EPOCH;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        // Version 15 (KIP-903) deprecated the plain `replica_id` - `"versions": "0-14"` in FetchRequest.json
        // @ 3.5.2 - and replaced it with the tagged `replica_state` at the end of the body, see below
        if (static::VERSION <= 14) {
            $body['replicaId'] = BinarySchema::TYPE_INT32;
        }
        $body['maxWaitTime'] = BinarySchema::TYPE_INT32;
        $body['minBytes']    = BinarySchema::TYPE_INT32;
        if (static::VERSION >= 3) {
            $body['maxBytes'] = BinarySchema::TYPE_INT32;
        }
        if (static::VERSION >= 4) {
            $body['isolationLevel'] = BinarySchema::TYPE_INT8;
        }
        if (static::VERSION >= 7) {
            $body['sessionId'] = BinarySchema::TYPE_INT32;
            $body['epoch']     = BinarySchema::TYPE_INT32;
        }
        // From version 13 the entries carry no name, so there is no field to index the array by, see
        // {@see self::$topicPartitions}
        $body['topicPartitions'] = static::VERSION >= 13
            ? [static::topicClass()]
            : ['topic' => static::topicClass()];
        if (static::VERSION >= 7) {
            $body['forgottenTopics'] = [static::forgottenTopicClass()];
        }
        if (static::VERSION >= 11) {
            $body['rackId'] = BinarySchema::TYPE_STRING;
        }
        // The `cluster_id` of version 12 is a TAGGED field (tag 0) and is therefore written at the end of the
        // body whatever its place in the json, and only when it is not the `null` of the specification
        if (static::VERSION >= 12) {
            $body['clusterId'] = new TaggedField(0, BinarySchema::TYPE_NULLABLE_STRING, null);
        }
        // The `replica_state` of version 15 is tagged as well (tag 1) and follows the cluster id in the section,
        // which the engine writes in ascending order of the tags whatever the scheme declares
        if (static::VERSION >= 15) {
            $body['replicaState'] = new TaggedField(1, FetchRequestReplicaState::class, null);
        }

        return $header + $body;
    }

    /**
     * Returns the node id this request claims to come from, -1 for the ordinary consumer fetch
     *
     * Below version 15 that is the plain `replica_id` field of the frame; from version 15 on it is the replica id
     * of the tagged `replica_state` of KIP-903, and a frame that does not carry the tag at all means the -1 of a
     * consumer, {@see self::$replicaState}.
     */
    public function getReplicaId(): int
    {
        if (static::VERSION >= 15) {
            return $this->replicaState?->replicaId ?? FetchRequestReplicaState::UNKNOWN;
        }

        return $this->replicaId;
    }

    /**
     * Returns the `replica_state` of version 15, `null` for a consumer and for every version below it (KIP-903)
     */
    public function getReplicaState(): ?FetchRequestReplicaState
    {
        return static::VERSION >= 15 ? $this->replicaState : null;
    }

    /**
     * Returns the isolation level this request asks for, {@see self::READ_UNCOMMITTED} below version 4
     */
    public function getIsolationLevel(): int
    {
        return static::VERSION >= 4 ? $this->isolationLevel : self::READ_UNCOMMITTED;
    }

    /**
     * Returns the session id and the epoch this request travels with, {@see FetchMetadata::legacy()} below version 7
     */
    public function getMetadata(): FetchMetadata
    {
        return static::VERSION >= 7 ? new FetchMetadata($this->sessionId, $this->epoch) : FetchMetadata::legacy();
    }

    /**
     * Returns the partitions this request asks the fetch session to forget, as topic => [partition, ...]
     *
     * @return array<string, list<int>>
     */
    public function getForgottenTopicPartitions(): array
    {
        $forgotten = [];
        foreach ($this->forgottenTopics as $forgottenTopic) {
            $forgotten[$forgottenTopic->topic] = $forgottenTopic->partitions;
        }

        return $forgotten;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this request is sent as
     *
     * @return class-string<FetchRequestTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 13 => FetchRequestTopic::class,
            static::VERSION >= 12 => FetchRequestTopicV12::class,
            static::VERSION >= 9  => FetchRequestTopicV9::class,
            static::VERSION >= 5  => FetchRequestTopicV5::class,
            default               => FetchRequestTopicV0::class,
        };
    }

    /**
     * Returns the class of a `forgotten_topics_data` entry for the version of the API that this request is sent as
     *
     * @return class-string<FetchRequestForgottenTopic>
     */
    protected static function forgottenTopicClass(): string
    {
        return static::VERSION >= 13 ? FetchRequestForgottenTopic::class : FetchRequestForgottenTopicV7::class;
    }

    /**
     * Returns the id this request has to write for a topic, and refuses a version 13 frame that has none
     *
     * A version below 13 names its topics by name and never looks at the map, so an unknown topic is the zero
     * uuid there; a version 13 frame can not name a topic at all without its id, which is the whole point of
     * KIP-516, and a client that does not know it refreshes its metadata instead of guessing.
     *
     * @param array<string, string> $topicIds Id of every topic, as name => the 16 raw bytes of its uuid
     *
     * @throws UnknownTopicIdException If a version 13 request names a topic whose id the caller did not state
     */
    private static function idOf(array $topicIds, string $topic): string
    {
        $topicId = $topicIds[$topic] ?? Uuid::ZERO;
        if (static::VERSION >= 13 && Uuid::isZero($topicId)) {
            throw new UnknownTopicIdException(
                [
                    'error' => 'A Fetch request of version 13 names its topics by id (KIP-516), and this client'
                        . ' does not know the id of this one yet',
                    'topic' => $topic,
                ]
            );
        }

        return $topicId;
    }

    /**
     * Returns the id of every topic this request names, as topic name => the 16 raw bytes of its uuid
     *
     * @return array<string, string>
     */
    public function getTopicIds(): array
    {
        return $this->topicIds;
    }
}
