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
 * DeleteRecords response object, version 1 (key 21)
 *
 * <pre>
 *   DeleteRecords Response (Version: 0 and 1) => throttle_time_ms [topics]
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
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `DELETE_RECORDS_RESPONSE_V1 =
 * DELETE_RECORDS_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see DeleteRecordsResponseV0} is the same frame with the version field of Kafka 0.11.
 *
 * @see docs/protocol/2.8.md, section "DeleteRecords API (key 21, v0 and v1)"
 */
class DeleteRecordsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

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
