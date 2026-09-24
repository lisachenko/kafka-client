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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FetchResponseNodeEndpoint;
use Protocol\Kafka\Protocol\Data\FetchResponseTopic;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV0;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV11;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV12;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV4;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV5;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * Fetch response object (key 1), version 17
 *
 * <pre>
 *   FetchResponse (Version: 16) => ThrottleTimeMs ErrorCode SessionId
 *                                 [TopicId [Partition ErrorCode HighwaterMarkOffset
 *                                             LastStableOffset LogStartOffset
 *                                             [AbortedTransactions] PreferredReadReplica
 *                                             RecordSetSize RecordSet TAG_BUFFER] TAG_BUFFER] TAG_BUFFER
 *     ThrottleTimeMs      => int32
 *     ErrorCode           => int16      -- since version 7
 *     SessionId           => int32      -- since version 7
 *     TopicName           => string    -- versions 0 to 12 only
 *     TopicId             => uuid      -- since version 13
 *     Partition           => int32
 *     ErrorCode           => int16
 *     HighwaterMarkOffset => int64
 *     LastStableOffset    => int64
 *     LogStartOffset      => int64
 *     AbortedTransactions => nullable [ProducerId int64 FirstOffset int64]
 *     PreferredReadReplica => int32     -- since version 11
 *     RecordSetSize       => int32
 *     DivergingEpoch      => tag 0, [Epoch int32 EndOffset int64] -- since version 12
 *     CurrentLeader       => tag 1, [LeaderId int32 LeaderEpoch int32] -- since version 12
 *     SnapshotId          => tag 2, [EndOffset int64 Epoch int32] -- since version 12
 *     NodeEndpoints       => tag 0 of the BODY, [NodeId int32 Host compact string Port int32
 *                            Rack compact nullable string] -- since version 16
 * </pre>
 *
 * Version 1 of the API added `ThrottleTimeMs` **before** the topics array - the opposite end of the response from
 * where the Produce API put its `ThrottleTime` (`FETCH_RESPONSE_V1` in `FetchResponse.schemaVersions()` @ 1.1.1,
 * `FetchResponse.readFrom` in `kafka/api/FetchResponse.scala`, which reads the field only when the request version
 * is greater than 0). It is the number of milliseconds the broker delayed the request because the client exceeded
 * its fetch quota; a broker without quotas - the default - always answers 0.
 *
 * The versions 2 and 3 did not change the frame at all (`FETCH_RESPONSE_V3` is `FETCH_RESPONSE_V2` is
 * `FETCH_RESPONSE_V1`): version 2 only tells the broker that the client understands message format v1, so the
 * message sets of the answer are no longer converted down to format v0, and version 3 only added the request-level
 * `MaxBytes`, see {@see FetchRequest}.
 *
 * **Version 4 (Kafka 0.11.0, KIP-98)** added `LastStableOffset` and the nullable `AbortedTransactions` array to
 * every partition entry and answers with the log as it lies, i.e. with record batches of the message format v2;
 * **version 5** (KIP-107) added `LogStartOffset` between the two, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition}. **Version 6** (Kafka 1.0) is the version 5 frame
 * again and only states that the client understands the error code 56 ({@see FetchResponseV6}).
 *
 * **Version 7 (Kafka 1.1, KIP-227)** is the next one that changed the frame: it inserts a **top-level error code**
 * and the **session id** between the throttle time and the topics array, see {@see self::$errorCode} and
 * {@see self::$sessionId}. Every version below 7 leaves both at 0, which is exactly what a version 7 answer to a
 * session-less request reports as well.
 *
 * **Version 8 (Kafka 2.0, KIP-219) changed the frame no more than 2, 3 and 6 did**: `FetchResponse.json` @ 2.8.2
 * carries no field of it, its comment is "Starting in version 8, on quota violation, brokers send out responses
 * before throttling", and {@see FetchResponseV7} decodes the very same bytes. What version 8 states is that the
 * client understands when a throttled answer arrives: at once, with the delay the broker is about to impose in
 * {@see self::$throttleTimeMs} and an **empty** topics array, and with the channel muted for that long afterwards.
 * The client has to wait the time out itself, see {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT}.
 *
 * **The versions 9 and 10 (Kafka 2.1) changed the answer no more than 8 did**: `FetchResponse.json` @ 2.8.2 has
 * no field of either, and {@see FetchResponseV9} and {@see FetchResponseV8} decode the very same bytes. What they
 * state is what the *request* promised - version 9 that the client sends a `current_leader_epoch` and understands
 * the codes **74** and **75** of KIP-320, version 10 that it understands a **zstd**-compressed record batch, which
 * a broker refuses to a lower version with **76** `UNSUPPORTED_COMPRESSION_TYPE` per partition.
 *
 * **Version 11 (Kafka 2.3, KIP-392)** put the `preferred_read_replica` between the aborted transactions and the
 * record set of every partition entry, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition::$preferredReadReplica}.
 *
 * **Version 12 (Kafka 2.7)** is the first **flexible** version, see {@see self::FLEXIBLE_VERSION}, and adds the
 * three **tagged** fields of a partition entry - `diverging_epoch` (KIP-595), `current_leader` and `snapshot_id`
 * (KIP-630). A ZooKeeper-backed broker fills none of them in for an ordinary consumer: they carry the answers of
 * the raft replication and of a leader that detected a divergence from the `last_fetched_epoch` of the request.
 *
 * **Version 13 (Kafka 3.1, KIP-516) replaces the topic name of every topic entry with the `topic_id`** the
 * request named it by, and nothing else: `FetchResponse.json` @ 3.1.2 declares `Topic` as `versions 0-12` and
 * `TopicId` as `13+`. The answer therefore never carries a topic name, and the two errors of the ids are the
 * per-partition **100** `UnknownTopicId` of an id the broker does not host and the top-level **106**
 * `FetchSessionTopicIdError` of a session that was started with the other kind of name, see {@see self::$errorCode}.
 * {@see FetchResponseV12} keeps the answer that names its topics.
 *
 * **The versions 14 and 15 (Kafka 3.5) leave the frame alone again.** `FetchResponse.json` @ 3.5.2 declares no
 * field of either - "Version 14 is the same as version 13 but it also receives a new error called
 * OffsetMovedToTieredStorageException (KIP-405)" and "Version 15 is the same as version 14 (KIP-903)" - so
 * {@see FetchResponseV14} and {@see FetchResponseV13} decode the very same bytes as this class. What version 14
 * states is that the client understands the error code **109** `OFFSET_MOVED_TO_TIERED_STORAGE` in a partition
 * entry, which a broker with remote storage answers for a fetch offset that is no longer on its local disk and
 * which it turns into **1** `OffsetOutOfRange` for a request below that version
 * (`ReplicaManager.handleOffsetMovedToTieredStorage` @ 3.9.2); what version 15 states is what the *request*
 * carries, the `replica_state` of KIP-903, see {@see FetchRequest::$replicaState}.
 *
 * **Version 16 (Kafka 3.7, KIP-951) puts the endpoints of those leaders into the body.** `FetchResponse.json`
 * @ 3.7.2 declares a top-level `NodeEndpoints` array, `"versions": "16+", "taggedVersions": "16+", "tag": 0`, with
 * "Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER &
 * FENCED_LEADER_EPOCH": the node id, the host, the port and the nullable rack of every node that a
 * `current_leader` of this answer points at, see {@see self::$nodeEndpoints}. The partition entry is unchanged -
 * the `current_leader` it names them with has been there since version 12 - and the request of version 16 is the
 * request of version 15, so an answer that refused nothing is the version 15 answer with another correlation of
 * versions and an empty tagged section ({@see FetchResponseV15} decodes those very bytes).
 *
 * **Version 17 (Kafka 3.9, KIP-853) declares nothing at all here**: `FetchResponse.json` @ 3.9.2 adds no field
 * and its whole comment is "Version 17 no changes to the response (KIP-853)", so this class decodes the very
 * bytes {@see FetchResponseV16} decodes. What the version added is in the *request*, the tagged
 * `replica_directory_id` of every partition entry, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::$replicaDirectoryId}.
 *
 * **Version 18 (Kafka 4.1, KIP-1166) declares nothing here either**: `FetchResponse.json` @ 4.1.0, "Version 18 no
 * changes to the response (KIP-1166)", so this class decodes the very bytes {@see FetchResponseV17} decodes; what
 * the version added is the tagged `high_watermark` of a follower in the *request*, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::$highWatermark}.
 *
 * What the answer of every version has to match is the *version of the request it belongs to*, which is why
 * {@see FetchResponseV17}, {@see FetchResponseV16}, {@see FetchResponseV15}, {@see FetchResponseV14}, {@see FetchResponseV13}, {@see FetchResponseV12}, {@see FetchResponseV11},
 * {@see FetchResponseV10}, {@see FetchResponseV9}, {@see FetchResponseV8},
 * {@see FetchResponseV7}, {@see FetchResponseV6}, {@see FetchResponseV5}, {@see FetchResponseV4},
 * {@see FetchResponseV3}, {@see FetchResponseV2}, {@see FetchResponseV1} and {@see FetchResponseV0} exist - the version constant selects
 * both the fields of the answer and the class of a partition entry.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)", "Fetch sessions (v7, KIP-227)",
 *      "The topic ids of the fetch path (v13, KIP-516)", "The tiered-storage error of KIP-405 (v14)",
 *      "The leader discovery of KIP-951 (v16)", "The replica directory id of KIP-853 (v17)" and
 *      "The high watermark of a follower, KIP-1166 (v18)"
 */
class FetchResponse extends AbstractResponse
{
    /**
     * Version of the Fetch API that this class decodes the answer of
     */
    public const int VERSION = 18;

    /**
     * First version of this api whose frame is written with the compact types and the tagged fields of KIP-482
     *
     * `FetchResponse.json` @ 3.1.2 declares `"flexibleVersions": "12+"`: the answer carries the response header
     * **v1**, compact strings, compact arrays and a **compact record set**, and a tagged-field section at the
     * end of the body, of every topic entry and of every partition entry - the section the three fields of
     * version 12 travel in, see {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition::$divergingEpoch}.
     */
    public const int FLEXIBLE_VERSION = 12;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the fetch **session**, zero when the request had none or the session is intact.
     *
     * This is the only top-level error code the Fetch api has, and it is about the session alone: 70
     * `FetchSessionIdNotFoundException` for an incremental request whose session id the broker does not know,
     * 71 `InvalidFetchSessionEpochException` for one whose epoch does not match the one the broker expects, and,
     * since Kafka 3.1, **106** `FetchSessionTopicIdError` for a session whose topics are named by id in one
     * request and by name in another (KIP-516). All three are retriable and all three are answered with an
     * **empty topics array**, so a client that receives one has to start over with a full fetch,
     * {@see FetchMetadata::nextCloseExisting()}.
     *
     * An answer below version 7 does not carry the field and leaves the 0, which is also what a version 7 answer
     * to a session-less request reports.
     *
     * @since Version 7 of protocol
     */
    public int $errorCode = 0;

    /**
     * Id of the fetch session this answer belongs to, 0 when there is none.
     *
     * A full fetch with the epoch 0 is answered with the id of the session the broker created for it; every
     * incremental fetch of that session is answered with the same id; a session-less request, a request that
     * closed its session, and every request whose session the broker could not create is answered with
     * {@see FetchMetadata::INVALID_SESSION_ID}, and so is a session error - `SessionErrorContext` @ 1.1.1 answers
     * the codes 70 and 71 with the session id 0, not with the id of the request.
     *
     * @since Version 7 of protocol
     */
    public int $sessionId = FetchMetadata::INVALID_SESSION_ID;

    /**
     * Fetch result of each requested topic, indexed by the topic name below version 13, a plain list above it
     *
     * A version 13 answer names its topics by the 16 raw bytes of their id and by nothing else, which is no
     * usable array key, so such an answer decodes into a list; the client resolves the ids against the map it
     * built the request from, see {@see \Protocol\Kafka\Common\Cluster::topicNameById()}.
     *
     * @var array<array-key, FetchResponseTopic>
     */
    public array $topics = [];

    /**
     * Where the leaders this answer named can be reached, as node id => endpoint (KIP-951)
     *
     * The top-level **tagged** field (tag 0) that version 16 added, the other half of the
     * {@see \Protocol\Kafka\Protocol\Data\FetchResponseCurrentLeader} a partition entry has carried since
     * version 12: that structure names the node id and the epoch of the real leader, this array the host and the
     * port they belong to, so that a consumer can follow the hint **without** a Metadata round trip.
     *
     * The broker writes it for a partition it refused with **6** `NotLeaderForPartition` or **74**
     * `FencedLeaderEpoch` and whose leader it knows (`KafkaApis.handleFetchRequest` @ 3.9.2, the
     * `versionId >= 16` branch), each node once. Its default is the **empty array**, so every ordinary answer
     * leaves it off the wire, and an answer below version 16 has no room for it at all.
     *
     * @since Version 16 of protocol (Kafka 3.7, KIP-951)
     *
     * @var array<int, FetchResponseNodeEndpoint>
     */
    public array $nodeEndpoints = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 1) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        if (static::VERSION >= 7) {
            $body['errorCode'] = BinarySchema::TYPE_INT16;
            $body['sessionId'] = BinarySchema::TYPE_INT32;
        }
        // From version 13 the entries carry no name, so there is no field to index the array by
        $body['topics'] = static::VERSION >= 13
            ? [static::topicClass()]
            : ['topic' => static::topicClass()];
        // The `node_endpoints` of version 16 is a TAGGED field (tag 0): it travels at the end of the body and
        // only when the broker really named a leader in one of the partition entries
        if (static::VERSION >= 16) {
            $body['nodeEndpoints'] = new TaggedField(0, ['nodeId' => FetchResponseNodeEndpoint::class], []);
        }

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<FetchResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 13 => FetchResponseTopic::class,
            static::VERSION >= 12 => FetchResponseTopicV12::class,
            static::VERSION >= 11 => FetchResponseTopicV11::class,
            static::VERSION >= 5  => FetchResponseTopicV5::class,
            static::VERSION >= 4  => FetchResponseTopicV4::class,
            default               => FetchResponseTopicV0::class,
        };
    }
}
