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

use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopicV0;

/**
 * Offsets (ListOffset) response object (key 2, v1)
 *
 * <pre>
 *   ListOffsets Response (Version: 1) => [responses]
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
 * array of {@see OffsetsResponseV0} is gone. The error codes a 0.10.2.2 broker reports here are:
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
 * and -1 (`KafkaApis.handleOffsetRequestV1` @ 0.10.2.2).
 *
 * @see docs/protocol/0.10.2.md, section "Offsets API (key 2, v0 and v1), a.k.a. ListOffset"
 */
class OffsetsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

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

        return $header + [
            'topics' => ['topic' => static::topicClass()],
        ];
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
