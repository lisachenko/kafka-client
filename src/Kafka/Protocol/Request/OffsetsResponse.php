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

/**
 * Offsets (ListOffset) response object (key 2, v2)
 *
 * <pre>
 *   ListOffsets Response (Version: 2) => throttle_time_ms [responses]
 *     throttle_time_ms => INT32     -- since version 2
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition error_code timestamp offset
 *         partition  => INT32
 *         error_code => INT16
 *         timestamp  => INT64
 *         offset     => INT64
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
 * A target timestamp that no message matches - one above the timestamp of every message of the log, and any
 * timestamp on an empty log - is **not** an error: the broker answers the code 0 with
 * {@see OffsetsResponsePartition::UNKNOWN_TIMESTAMP} and {@see OffsetsResponsePartition::UNKNOWN_OFFSET}, i.e. -1
 * and -1 (`KafkaApis.fetchOffsetForTimestamp` @ 0.11.0.3).
 *
 * @see docs/protocol/0.11.0.md, sections "Offsets API (key 2, v0, v1 and v2), a.k.a. ListOffset" and
 *      "Quotas and throttle time"
 */
class OffsetsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

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
        return static::VERSION >= 1 ? OffsetsResponseTopic::class : OffsetsResponseTopicV0::class;
    }
}
