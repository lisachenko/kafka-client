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

use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopicV1;

/**
 * Offsets API (key 2, v9), a.k.a. ListOffset
 *
 * This API describes the valid offset range available for a set of topic-partitions. As with the produce and fetch
 * APIs requests must be directed to the broker that is currently the leader for the partitions in question. This can
 * be determined using the metadata API.
 *
 * <pre>
 *   ListOffsets Request (Version: 10) => replica_id isolation_level [topics] timeout_ms
 *     replica_id      => INT32
 *     isolation_level => INT8       -- since version 2
 *     topics          => topic [partitions]
 *       topic      => STRING
 *       partitions => partition current_leader_epoch timestamp
 *         partition            => INT32
 *         current_leader_epoch => INT32     -- since version 4
 *         timestamp            => INT64
 *     timeout_ms      => INT32      -- since version 10
 * </pre>
 *
 * Kafka 0.10.1 added version 1 with KIP-79, on top of the message timestamps of the format v1: the broker now
 * knows when every message was written, keeps a time index next to the offset index of each log segment, and can
 * therefore answer "the offset of the first message whose timestamp is `>= t`" instead of the segment start offsets
 * that {@see OffsetsRequestV0} asks for. One partition gets one offset, so the `max_num_offsets` of version 0 is
 * gone, and the answer names the timestamp of the message it found.
 *
 * Version 2 (KIP-98, Kafka 0.11) inserted `isolation_level` between the replica id and the topics, and it is the
 * transactional half of the api: with {@see \Protocol\Kafka\Protocol\Request\FetchRequest::READ_UNCOMMITTED} - the
 * default, and what every version below 2 gets - {@see self::LATEST} is the log end offset, while with
 * `READ_COMMITTED` it is the **last stable offset**, i.e. the offset of the first record that belongs to a
 * transaction that has neither committed nor aborted yet (`KafkaApis.fetchOffsetForTimestamp` @ 0.11.0.3 passes
 * the isolation level down to `Log.fetchOffsetsByTimestamp`). A consumer that reads committed data must therefore
 * ask for its end offsets with the same isolation level it fetches with, or it waits for records it will never be
 * shown. {@see OffsetsRequestV1} and {@see OffsetsRequestV0} do not put the field on the wire at all.
 *
 * **Version 3 (Kafka 2.0, KIP-219) is byte-identical to version 2** in both directions: `ListOffsetsRequest.json`
 * @ 2.8.2 has no field of it and its comment is "Version 3 is the same as version 2". What it states is that the
 * **client** honours the `throttle_time_ms` of the answer itself, because a throttled request is answered first
 * and the channel is muted for the reported time afterwards instead of the answer being held back, see
 * {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT}. {@see OffsetsRequestV2} keeps version 2, which a
 * 2.8.2 broker throttles in exactly the same way - the version is the promise of the client, not a switch of the
 * broker.
 *
 * **Version 4 (Kafka 2.1, KIP-320) put a `current_leader_epoch` into every partition entry**, between the
 * partition index and the target timestamp, and a `leader_epoch` into every entry of the answer, see
 * {@see \Protocol\Kafka\Protocol\Data\OffsetsRequestPartition::$currentLeaderEpoch} and
 * {@see \Protocol\Kafka\Protocol\Data\OffsetsResponsePartition::$leaderEpoch}. The two halves are what lets a
 * consumer seek without reading past a leader change: it sends the epoch it believes the partition is led with -
 * and is answered **74** or **75** when that belief is stale - and it stores the epoch of the offset it got, to
 * send it back with its next fetch. {@see OffsetsRequestV3} keeps the version that carries neither.
 *
 * **Version 5 (Kafka 2.2, KIP-207) sends the very same frame once more** - `ListOffsetsRequest.json` @ 2.8.2:
 * "Version 5 is the same as version 4" - and what it states is that the client understands **one more error
 * code** in the answer: **78** `OFFSET_NOT_AVAILABLE`. A leader that was elected moments ago may hold a high
 * watermark that is still below the start offset of its own epoch, and until it catches up it cannot say where
 * the end of the log is; `Partition.fetchOffsetForTimestamp` @ 2.8.2 raises the error for a **client** request
 * (a follower is exempt) that asks for {@see self::LATEST}, or for a timestamp whose answer would lie at or
 * beyond the last fetchable offset. `KafkaApis.handleListOffsetRequest` @ 2.8.2 then splits on the version:
 * `if (request.header.apiVersion >= 5)` the code 78 travels, otherwise the partition is answered **5**
 * `LEADER_NOT_AVAILABLE` - which is what every version up to {@see OffsetsRequestV4} sees, and which is
 * indistinguishable from "this partition has no leader at all". That distinction is the whole of KIP-207: both
 * codes are retriable, but 78 says "the leader is there and will know in a moment", so a client retries without
 * refreshing its metadata first. The frame is the version 4 frame with another number in its header.
 *
 * **Version 6 (Kafka 2.8) is the flexible version of KIP-482** and adds no field either, see
 * {@see self::FLEXIBLE_VERSION}: the same question with the request header **v2**, a compact topic name, compact
 * arrays and a tagged-field section at the end of the body, of every topic entry and of every partition entry.
 * {@see OffsetsRequestV5} keeps the plain frame.
 *
 * **Version 7 (Kafka 3.0, KIP-734) is the version 6 frame once more and one more question.**
 * `ListOffsetsRequest.json` @ 3.0.2 declares no field for it - "Version 7 enables listing offsets by max timestamp
 * (KIP-734)" is its whole comment - so {@see OffsetsRequestV6} writes the same bytes with another number in its
 * header. What the version buys is the third special target time {@see self::MAX_TIMESTAMP} (`-3`): "the offset of
 * the record with the **largest timestamp** of this partition", which is not the last offset of the log as soon as
 * the producer stamped its records out of order. A broker keeps that pair in the metadata of every log segment
 * (`maxTimestampSoFar`/`offsetOfMaxTimestampSoFar`), so the lookup reads no record, and unlike `-1` and `-2` the
 * answer carries a real timestamp next to the offset.
 *
 * The version is the promise the client makes: `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 maps every
 * negative target time to the version that introduced it (`timestampMinSupportedVersion`) and answers a partition
 * whose target time needs a higher version than the request with **35** `UNSUPPORTED_VERSION` - per partition,
 * with the timestamp and the offset -1. That is what a `-3` of {@see OffsetsRequestV6} gets, and it is also the
 * answer to any other negative value this line does not know.
 *
 * **Version 8 (Kafka 3.5, KIP-405) is the same frame again and a fourth question.**
 * `ListOffsetsRequest.json` @ 3.5.2 declares no field for it either - "Version 8 enables listing offsets by local
 * log start offset (KIP-405)" - so {@see OffsetsRequestV7} writes the same bytes with another number in its
 * header, and this class is the version 8. What it buys is the special target time
 * {@see self::EARLIEST_LOCAL_TIMESTAMP} (`-4`): "the first offset that is still on the **local** disk of this
 * broker", which is where reading from the local log begins once the older segments of the partition have been
 * moved to remote storage. Without tiered storage - `remote.log.storage.system.enable` is off on the node of this
 * line, as it is by default - the local log start offset **is** the log start offset, so `-4` answers exactly what
 * {@see self::EARLIEST} answers, and the value is how a client of a tiered cluster tells the two apart.
 *
 * **Version 9 (Kafka 3.9, KIP-1005) is that same frame a third time and a fifth question.**
 * `ListOffsetsRequest.json` @ 3.9.2 declares no field for it either - "Version 9 enables listing offsets by last
 * tiered offset (KIP-1005)" - so {@see OffsetsRequestV8} writes the same bytes with another number in its header,
 * and this class is the version 9. What it buys is the special target time
 * {@see self::LATEST_TIERED_TIMESTAMP} (`-5`): "the **last** offset that has been moved to remote storage", the
 * other end of the range `-4` names the beginning of. The two together are what tells a client of a tiered
 * cluster which part of a partition is served from the object store and which part from the disks of the broker.
 *
 * **Version 10 (Kafka 4.0, KIP-1075) appends a `timeout_ms` to the end of the body**, and this class is version
 * 10: `ListOffsetsRequest.json` @ 4.0.0, "Version 10 enables async remote list offsets support (KIP-1075)",
 * `{ "name": "TimeoutMs", "type": "int32", "versions": "10+", "ignorable": true }` - "the timeout to await a response
 * in milliseconds for requests that require reading from remote storage for topics enabled with tiered storage". A
 * broker of that release looks a timestamp up in the remote part of a log asynchronously and answers the partition
 * when the lookup is done or this timeout is over; a topic without remote storage - every topic of the node of this
 * line - is answered at once, whatever the value. The Java consumer sends its `request.timeout.ms`
 * (`OffsetFetcher.sendListOffsetRequest` @ 4.0.0), and so does {@see \Protocol\Kafka\Client}, see
 * {@see self::$timeoutMs}. {@see OffsetsRequestV9} keeps the version without it; the answer did not change.
 *
 * **Kafka 4.0 also removed version 0 (KIP-896)**: "Version 0 was removed in Apache Kafka 4.0, Version 1 is the new
 * baseline", and a 4.x node closes the connection on a frame of it. {@see OffsetsRequestV0} stays, for the wire
 * vectors of the lines below and for a peer of Kafka 3.x.
 *
 * The five special values keep their meaning in every version that knows them: {@see self::LATEST} (`-1`) asks for
 * the end of the log - the offset the next produced message will get, capped as the isolation level prescribes -
 * {@see self::EARLIEST} (`-2`) for the first offset that is still on disk, {@see self::MAX_TIMESTAMP} (`-3`,
 * version 7) for the offset of the record with the largest timestamp, {@see self::EARLIEST_LOCAL_TIMESTAMP}
 * (`-4`, version 8) for the start of the local log and {@see self::LATEST_TIERED_TIMESTAMP} (`-5`, version 9) for
 * the end of the tiered part of it. Only the third is answered with a real timestamp; the other four do not read
 * a message and are answered with the timestamp -1.
 *
 * @see docs/protocol/4.3.md, sections "Offsets API (key 2, v0 to v10), a.k.a. ListOffset", "The timeout of
 *      KIP-1075 (v10)" and "The leader epoch (KIP-320)"
 */
class OffsetsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSETS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 10;

    /**
     * First version of this api whose frame is written with the compact types and the tagged fields of KIP-482
     *
     * `ListOffsetsRequest.json` @ 2.8.2 declares `"flexibleVersions": "6+"` and its only comment on the version
     * is "Version 6 enables flexible versions": not a field was added, the encoding changed.
     */
    public const int FLEXIBLE_VERSION = 6;

    /**
     * Special value for the offset of the next coming message, `ListOffsetRequest.LATEST_TIMESTAMP` @ 0.10.2.2
     */
    public const int LATEST = -1;

    /**
     * Special value for receiving the earliest available offset, `ListOffsetRequest.EARLIEST_TIMESTAMP` @ 0.10.2.2
     */
    public const int EARLIEST = -2;

    /**
     * Special value for the offset of the record with the largest timestamp, `ListOffsetsRequest.MAX_TIMESTAMP`
     * @ 3.0.2 (Kafka 3.0, KIP-734), which **version 7 and above** of the api accept
     *
     * The answer names that largest timestamp and the offset of the record carrying it, which is the last offset
     * of the log only while the timestamps rise with the offsets - a producer that stamps its records out of order,
     * or several producers whose clocks differ, put it anywhere. A request below version 7 that asks for it is
     * answered **35** `UNSUPPORTED_VERSION` for that partition, see {@see OffsetsRequestV6}.
     *
     * @since Version 7 of protocol
     */
    public const int MAX_TIMESTAMP = -3;

    /**
     * Special value for the first offset that is still on the **local** disk of the broker,
     * `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP` @ 3.5.2 (Kafka 3.5, KIP-405), which **version 8 and above**
     * of the api accept
     *
     * On a cluster with tiered storage the front of a partition lives in remote storage and only the newest
     * segments are on the broker's own disk: `-2` answers where the *log* starts, this value answers where the
     * **local** log starts, i.e. the first offset a fetch is served from the local segments. Without remote
     * storage - which is how the node of this line runs - `UnifiedLog.localLogStartOffset` @ 3.9.2 is the log
     * start offset itself and the two answers are the same number. A request below version 8 that asks for it is
     * answered **35** `UNSUPPORTED_VERSION` for that partition, see {@see OffsetsRequestV7}.
     *
     * It is the `OffsetSpec.earliestLocal()` of the Java admin client, which has no consumer counterpart:
     * {@see \Protocol\Kafka\Admin\AdminClient::listEarliestLocalOffsets()} is where this client names it.
     *
     * @since Version 8 of protocol
     */
    public const int EARLIEST_LOCAL_TIMESTAMP = -4;

    /**
     * Special value for the **last** offset that has been moved to remote storage,
     * `ListOffsetsRequest.LATEST_TIERED_TIMESTAMP` @ 3.9.2 (Kafka 3.9, KIP-1005), which **version 9 and above** of
     * the api accept
     *
     * It is the other end of the range {@see self::EARLIEST_LOCAL_TIMESTAMP} names the beginning of: `-5` answers
     * where the **tiered** part of the partition stops, `-4` where the local part starts, and between the two
     * answers lies the overlap a broker keeps on both. `UnifiedLog.fetchOffsetByTimestamp` @ 3.9.2 answers it with
     * `highestOffsetInRemoteStorage()`, so a partition of a topic **without** remote storage - which is every
     * topic of the node of this line, `remote.log.storage.system.enable` being off - is answered the offset
     * `-1` with the timestamp `-1` and the error code 0: there is no tiered offset, and that is not an error.
     *
     * A request below version 9 that asks for it is answered **35** `UNSUPPORTED_VERSION` for that partition, see
     * {@see OffsetsRequestV8}.
     *
     * It is the `OffsetSpec.latestTiered()` of the Java admin client, which has no consumer counterpart:
     * {@see \Protocol\Kafka\Admin\AdminClient::listLatestTieredOffsets()} is where this client names it.
     *
     * @since Version 9 of protocol
     */
    public const int LATEST_TIERED_TIMESTAMP = -5;

    /**
     * Replica id of an ordinary consumer, `ListOffsetRequest.CONSUMER_REPLICA_ID` @ 0.10.2.2.
     *
     * A consumer never sees an offset above the high watermark of the partition: the broker caps the answer of such
     * a request at it, because everything above it is not replicated yet.
     */
    public const int CONSUMER_REPLICA_ID = -1;

    /**
     * Replica id that lets a non-broker read like a follower, `ListOffsetRequest.DEBUGGING_REPLICA_ID` @ 0.10.2.2.
     *
     * The answer is then the state of the local log, uncapped by the high watermark, and the request is served by
     * any replica of the partition instead of its leader alone.
     */
    public const int DEBUGGING_REPLICA_ID = -2;

    /**
     * Timeout of a remote lookup when the caller names none: the default `request.timeout.ms` of the Java consumer,
     * whose `OffsetFetcher.sendListOffsetRequest` @ 4.0.0 sends exactly that value (KIP-1075)
     */
    public const int DEFAULT_TIMEOUT_MS = 30000;

    /**
     * Topics to list the offsets of, indexed by the topic name
     *
     * @var array<string, OffsetsRequestTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * A value of the `$topicPartitions` map is either a plain target time or an already built topic DTO.
     *
     * @param array<string, array<int, int>|OffsetsRequestTopic> $topicPartitions Target time of every partition, as
     *        topic => partition => time, where the time is a timestamp in milliseconds, {@see self::LATEST},
     *        {@see self::EARLIEST}, {@see self::MAX_TIMESTAMP}, {@see self::EARLIEST_LOCAL_TIMESTAMP} or
     *        {@see self::LATEST_TIERED_TIMESTAMP}
     * @param int    $replicaId      The node id of the replica that initiates this request. Ordinary consumers send
     *                               {@see self::CONSUMER_REPLICA_ID}, as they have no node id.
     * @param int    $isolationLevel {@see FetchRequest::READ_UNCOMMITTED} or {@see FetchRequest::READ_COMMITTED},
     *                               not on the wire below version 2
     * @param string $clientId       Unique client identifier
     * @param int    $correlationId  Correlated request id
     * @param int    $timeoutMs      How long the broker may wait for a lookup in remote storage, not on the wire
     *                               below version 10
     */
    public function __construct(
        array $topicPartitions,
        protected readonly int $replicaId = self::CONSUMER_REPLICA_ID,
        /**
         * Whether the answer may name offsets of records that belong to an open transaction.
         *
         * @since Version 2 of protocol
         */
        protected readonly int $isolationLevel = FetchRequest::READ_UNCOMMITTED,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Milliseconds the broker may wait for a lookup that has to read from remote storage (KIP-1075).
         *
         * A partition of a topic with tiered storage whose target time lies in the remote part of its log is looked
         * up asynchronously from Kafka 4.0 on, and this is how long the broker waits for that lookup before it
         * answers the partition with **7** `REQUEST_TIMED_OUT` (`DelayedRemoteListOffsets` @ 4.0.0); a topic without
         * remote storage is answered at once. A value that is not above 0 leaves the wait to the broker's
         * `remote.list.offsets.request.timeout.ms` (`ReplicaManager.fetchOffset` @ 4.0.0). The field is the last
         * one of the body.
         *
         * @since Version 10 of protocol (Kafka 4.0, KIP-1075)
         */
        protected readonly int $timeoutMs = self::DEFAULT_TIMEOUT_MS
    ) {
        $topicClass            = static::topicClass();
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitionTimestamps) {
            $packedTopicPartitions[$topic] = $partitionTimestamps instanceof OffsetsRequestTopic
                ? $partitionTimestamps
                : new $topicClass((string) $topic, $partitionTimestamps);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds a request that asks for the same target time for each of the given topic partitions
     *
     * @param iterable<TopicPartition> $topicPartitions Partitions to list the offsets of
     * @param int                      $timestamp       Timestamp in ms, {@see self::LATEST},
     *                                                  {@see self::EARLIEST} or {@see self::MAX_TIMESTAMP}
     */
    public static function fromTopicPartitions(
        iterable $topicPartitions,
        int $timestamp = self::LATEST,
        int $replicaId = self::CONSUMER_REPLICA_ID,
        int $isolationLevel = FetchRequest::READ_UNCOMMITTED,
        string $clientId = '',
        int $correlationId = 0,
        int $timeoutMs = self::DEFAULT_TIMEOUT_MS
    ): static {
        return new static(
            self::partitionTimestamps($topicPartitions, $timestamp),
            $replicaId,
            $isolationLevel,
            $clientId,
            $correlationId,
            $timeoutMs
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'replicaId' => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 2) {
            $body['isolationLevel'] = BinarySchema::TYPE_INT8;
        }
        $body['topicPartitions'] = ['topic' => static::topicClass()];
        // KIP-1075 put the timeout of a remote lookup at the very END of the body, behind the topics
        if (static::VERSION >= 10) {
            $body['timeoutMs'] = BinarySchema::TYPE_INT32;
        }

        return $header + $body;
    }

    /**
     * Returns how long the broker may wait for a lookup in remote storage, in milliseconds (KIP-1075)
     */
    public function getTimeoutMs(): int
    {
        return $this->timeoutMs;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class sends
     *
     * @return class-string<OffsetsRequestTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 4 => OffsetsRequestTopic::class,
            static::VERSION >= 1 => OffsetsRequestTopicV1::class,
            default              => OffsetsRequestTopicV0::class,
        };
    }

    /**
     * Spreads one target time over every one of the given topic partitions
     *
     * @param iterable<TopicPartition> $topicPartitions Partitions to list the offsets of
     *
     * @return array<string, array<int, int>> Target time as topic => partition => time
     */
    protected static function partitionTimestamps(iterable $topicPartitions, int $timestamp): array
    {
        $partitionTimestamps = [];
        foreach ($topicPartitions as $topicPartition) {
            $partitionTimestamps[$topicPartition->topic][$topicPartition->partition] = $timestamp;
        }

        return $partitionTimestamps;
    }
}
