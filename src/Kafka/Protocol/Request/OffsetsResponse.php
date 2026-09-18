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
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopicV1;

/**
 * Offsets (ListOffset) response object (key 2, v9)
 *
 * <pre>
 *   ListOffsets Response (Version: 9) => throttle_time_ms [responses]
 *     throttle_time_ms => INT32     -- since version 2
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition error_code timestamp offset leader_epoch
 *         partition    => INT32
 *         error_code   => INT16
 *         timestamp    => INT64
 *         offset       => INT64
 *         leader_epoch => INT32     -- since version 4
 * </pre>
 *
 * Version 1 answers one offset per partition together with the timestamp of the message it points at; the offset
 * array of {@see OffsetsResponseV0} is gone. Version 2 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of
 * the topics array and left the partitions alone, so {@see OffsetsResponseV1} decodes the same entries without it.
 * The error codes a 0.11.0.3 broker reports here are:
 *
 * | Code | Name                       | Meaning                                                                  |
 * |------|----------------------------|--------------------------------------------------------------------------|
 * | 0    | None                       | The lookup succeeded - which includes "no message matches", see below     |
 * | 3    | UnknownTopicOrPartition    | The cluster does not host this partition                                  |
 * | 6    | NotLeaderForPartition      | This broker is not the leader of the partition any more                   |
 * | 42   | InvalidRequest             | The same partition appears twice in one request                           |
 * | 43   | UnsupportedForMessageFormat| A timestamp lookup on a topic whose `message.format.version` is below 0.10 |
 *
 * **Version 3 (Kafka 2.0, KIP-219) changed nothing about the frame**: `ListOffsetsResponse.json` @ 2.8.2 has no
 * field of it and {@see OffsetsResponseV2} decodes the very same bytes. What version 3 states is that the client
 * understands when a throttled answer arrives - first, with the delay it reports, and the channel muted
 * afterwards, see {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT}.
 *
 * **Version 4 (Kafka 2.1, KIP-320) appended `leader_epoch` to every partition entry**, the epoch the leader was
 * on when it read the answered offset, see
 * {@see \Protocol\Kafka\Protocol\Data\OffsetsResponsePartition::$leaderEpoch}; {@see OffsetsResponseV3} keeps
 * the frame that ends with the offset.
 *
 * **Version 5 (Kafka 2.2, KIP-207) adds no field and one error code.** `ListOffsetsResponse.json` @ 2.8.2 says
 * "Version 5 adds a new error code, OFFSET_NOT_AVAILABLE", so {@see OffsetsResponseV4} decodes the very same
 * bytes; what changes is what a partition entry may carry. **78** `OffsetNotAvailableException` is the answer of
 * a leader whose high watermark has not caught up with the start offset of the epoch it was elected in - it
 * cannot tell where the end of its log is yet - and it replaces the **5** `LEADER_NOT_AVAILABLE` that every
 * version below 5 is answered in that state, see {@see OffsetsRequest}. Both are retriable; 78 is the one that
 * says the leader exists, so a client simply asks again instead of refreshing its metadata first.
 *
 * **Version 6 (Kafka 2.8) is the flexible version of KIP-482**, see {@see self::FLEXIBLE_VERSION}: the response
 * header **v1**, a compact topic name, compact arrays and a tagged-field section at the end of the body, of every
 * topic entry and of every partition entry, with the same fields as version 5. {@see OffsetsResponseV5} decodes
 * the plain frame.
 *
 * **Version 7 (Kafka 3.0, KIP-734) is the version 6 answer, field for field.** `ListOffsetsResponse.json` @ 3.0.2
 * says "Version 7 is the same as version 6 (KIP-734)" and declares no field of it, so {@see OffsetsResponseV6}
 * decodes the very same bytes. What the version carries is the answer to the new target time
 * {@see OffsetsRequest::MAX_TIMESTAMP} (`-3`): the **largest timestamp** of the partition in `timestamp` and the
 * offset of the record that holds it in `offset`, which is the last offset of the log only while the timestamps
 * of the records rise with their offsets. It is the one lookup of this api whose answer carries a real timestamp
 * without reading a record - the broker keeps the pair in the metadata of every log segment - and the one for
 * which an empty partition is answered `-1` / `-1` with the error code 0, exactly like a timestamp nothing
 * matches. A **version 6 or lower** request that asks for `-3` is answered **35** `UNSUPPORTED_VERSION` for that
 * partition, with `-1` / `-1`, see {@see OffsetsRequestV6}.
 *
 * **Version 8 (Kafka 3.5, KIP-405) is that same answer once more.** `ListOffsetsResponse.json` @ 3.5.2 says
 * "Version 8 enables listing offsets by local log start offset" and declares no field of it, so
 * {@see OffsetsResponseV7} decodes the very same bytes. What the version carries is the answer to the target time
 * {@see OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP} (`-4`): the first offset that is still on the **local** disk of
 * the broker, with the timestamp `-1`, because that lookup reads no record either. On a cluster without remote
 * storage it is the log start offset, i.e. the same number `-2` answers. A **version 7 or lower** request that
 * asks for `-4` is answered **35** `UNSUPPORTED_VERSION` for that partition, see {@see OffsetsRequestV7}.
 *
 * **Version 9 (Kafka 3.9, KIP-1005) is that answer a third time.** `ListOffsetsResponse.json` @ 3.9.2 says
 * "Version 9 enables listing offsets by last tiered offset" and declares no field of it, so
 * {@see OffsetsResponseV8} decodes the very same bytes. What the version carries is the answer to the target time
 * {@see OffsetsRequest::LATEST_TIERED_TIMESTAMP} (`-5`): the last offset that has been moved to remote storage,
 * with the timestamp `-1`. A partition of a topic without remote storage - every topic of the node of this line -
 * has no such offset and is answered the error code 0 with the offset **-1** and the leader epoch -1, which is
 * the one answer of this api that says "the question is valid and there is nothing to report". A **version 8 or
 * lower** request that asks for `-5` is answered **35** `UNSUPPORTED_VERSION` for that partition, see
 * {@see OffsetsRequestV8}.
 *
 * A target timestamp that no message matches - one above the timestamp of every message of the log, and any
 * timestamp on an empty log - is **not** an error: the broker answers the code 0 with
 * {@see OffsetsResponsePartition::UNKNOWN_TIMESTAMP} and {@see OffsetsResponsePartition::UNKNOWN_OFFSET}, i.e. -1
 * and -1 (`KafkaApis.fetchOffsetForTimestamp` @ 0.11.0.3).
 *
 * @see docs/protocol/3.9.md, sections "Offsets API (key 2, v0 to v9), a.k.a. ListOffset",
 *      "Quotas and throttle time" and "The leader epoch (KIP-320)"
 */
class OffsetsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;

    /**
     * First version of this api whose frame is written with the compact types and the tagged fields of KIP-482
     *
     * `ListOffsetsResponse.json` @ 2.8.2 declares `"flexibleVersions": "6+"`; not a field was added to the
     * answer, the encoding changed.
     */
    public const int FLEXIBLE_VERSION = 6;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 2 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Offsets for each of the requested topics, indexed by the topic name
     *
     * @var array<string, OffsetsResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 2) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['topics'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<OffsetsResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 4 => OffsetsResponseTopic::class,
            static::VERSION >= 1 => OffsetsResponseTopicV1::class,
            default              => OffsetsResponseTopicV0::class,
        };
    }
}
