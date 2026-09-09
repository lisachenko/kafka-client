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
use Protocol\Kafka\Protocol\Data\DeleteRecordsResponseTopic;

/**
 * DeleteRecords response object, version 0 (key 21)
 *
 * <pre>
 *   DeleteRecords Response (Version: 0) => throttle_time_ms [topics]
 *     throttle_time_ms => INT32
 *     topics           => topic [partitions]
 *       topic      => STRING
 *       partitions => partition low_watermark error_code
 *         partition     => INT32
 *         low_watermark => INT64
 *         error_code    => INT16
 * </pre>
 *
 * The api was born in Kafka 0.11, after KIP-124 made `throttle_time_ms` the first field of every new answer, so
 * there is no version of it without one - unlike Produce and Fetch, where the field arrived with a later version.
 *
 * Error codes a 0.11.0.3 broker reports per partition:
 *
 * | Code | Name                     | Meaning                                                                     |
 * |------|--------------------------|-----------------------------------------------------------------------------|
 * | 0    | None                     | The records below the offset were deleted, `lowWatermark` is the new start   |
 * | 1    | OffsetOutOfRange         | The offset is above the high watermark of the partition                     |
 * | 3    | UnknownTopicOrPartition  | The metadata cache of the broker has no such topic, or the topic has no such partition |
 * | 6    | NotLeaderForPartition    | The broker that was asked does not lead the partition                       |
 * | 7    | RequestTimedOut          | The new low watermark was not acknowledged by the ISR within `timeout`      |
 * | 29   | TopicAuthorizationFailed | The client may describe the topic but not delete from it                    |
 * | 42   | InvalidRequest           | A negative offset other than -1                                             |
 *
 * @see docs/protocol/0.11.0.md, section "DeleteRecords API (key 21, v0)"
 */
class DeleteRecordsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every requested topic, indexed by the topic name
     *
     * @var array<string, DeleteRecordsResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'topics'         => ['topic' => DeleteRecordsResponseTopic::class],
        ];
    }
}
