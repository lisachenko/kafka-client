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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;

/**
 * OffsetFetch response object, version 3 (Kafka 0.11)
 *
 * <pre>
 *   OffsetFetch Response (Version: 3) => throttle_time_ms [responses] error_code
 *     throttle_time_ms => INT32     -- since version 3
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition offset metadata error_code
 *         partition  => INT32
 *         offset     => INT64
 *         metadata   => NULLABLE_STRING
 *         error_code => INT16
 *     error_code => INT16           -- since version 2
 * </pre>
 *
 * Version 2 appended a **group-level** `error_code` **after** the topics array. It reports what is wrong with the
 * group rather than with one of its partitions - 15 (GroupCoordinatorNotAvailable), 16 (NotCoordinatorForGroup), 14
 * (GroupLoadInProgress) or 30 (GroupAuthorizationFailed) - and the answer that carries it has an **empty** topics
 * array: `OffsetFetchRequest.getErrorResponse()` @ 0.11.0.3 fills the partitions only for the versions below 2,
 * which had nowhere else to put a group error. The per-partition codes stay what they were.
 *
 * Version 3 (KIP-124, Kafka 0.11) added the leading `throttle_time_ms` and changed nothing else, so this answer
 * carries an error code at each of its two ends: the throttle time, the topics, and then the group error.
 *
 * The lower versions each have a class of their own: {@see OffsetFetchResponseV2} still reads the group error code,
 * {@see OffsetFetchResponseV1} and {@see OffsetFetchResponseV0} do not have it and report
 * {@see KafkaException::NO_ERROR} here, because the whole answer of those versions is made of per-partition
 * results.
 *
 * @see docs/protocol/0.11.0.md, sections "OffsetFetch API (key 9, v0 to v3)" and "Quotas and throttle time"
 */
class OffsetFetchResponse extends AbstractResponse
{
    /**
     * Version of the OffsetFetch API that this class decodes the answer of
     */
    public const int VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 3 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * List of topic responses
     *
     * @var array<string, OffsetFetchResponseTopic>
     */
    public array $topics = [];

    /**
     * Error of the group itself, which the coordinator reports instead of any topic at all
     *
     * @since Version 2 of protocol
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 3) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['topics'] = ['topic' => OffsetFetchResponseTopic::class];
        if (static::VERSION >= 2) {
            $body['errorCode'] = BinarySchema::TYPE_INT16;
        }

        return $header + $body;
    }
}
