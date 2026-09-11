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
 * Offsets (ListOffset) response object (key 2, v6)
 *
 * <pre>
 *   ListOffsets Response (Version: 6) => throttle_time_ms [responses]
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
 * A target timestamp that no message matches - one above the timestamp of every message of the log, and any
 * timestamp on an empty log - is **not** an error: the broker answers the code 0 with
 * {@see OffsetsResponsePartition::UNKNOWN_TIMESTAMP} and {@see OffsetsResponsePartition::UNKNOWN_OFFSET}, i.e. -1
 * and -1 (`KafkaApis.fetchOffsetForTimestamp` @ 0.11.0.3).
 *
 * @see docs/protocol/2.8.md, sections "Offsets API (key 2, v0 to v6), a.k.a. ListOffset",
 *      "Quotas and throttle time" and "The leader epoch (KIP-320)"
 */
class OffsetsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;

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
