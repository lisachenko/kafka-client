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
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsCursor;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsResponseTopic;
use Protocol\Kafka\Protocol\NullableStruct;

/**
 * DescribeTopicPartitions response object, version 0 (key 75, Kafka 3.8, KIP-966)
 *
 * <pre>
 *   DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor
 *     throttle_time_ms => INT32
 *     topics           => error_code name topic_id is_internal [partitions] topic_authorized_operations
 *     next_cursor      => topic_name partition_index      -- nullable
 * </pre>
 *
 * **There is no top-level error code**: every topic carries one of its own, as in a Metadata answer. What a
 * caller has to read instead is the `next_cursor`, which is the whole point of the api - a `null` one means the
 * answer is complete, and one that is there means the page ended and names the topic and partition the **next**
 * request has to start at. The node ends a page in two places: in the middle of a topic whose partitions did not
 * fit (`next_cursor` names that topic and the first partition that is missing) and in front of a topic that does
 * not fit at all (`next_cursor` names that topic and the partition 0).
 *
 * **The topics of the answer are sorted by name** - `DescribeTopicPartitionsRequestHandler` @ 3.9.2 sorts the
 * requested names before it asks the metadata cache - which is what makes the cursor a stable position at all:
 * the order of the request is lost, and a client that pages has to merge by name.
 *
 * An entry that was refused (**29** `TopicAuthorizationFailed`) is appended **behind** the sorted ones, because
 * the handler collects the unauthorized topics in a set of their own and adds them to the answer at the end.
 *
 * @see docs/protocol/4.3.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
class DescribeTopicPartitionsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Topics of this page, indexed by the topic name
     *
     * @var array<string, DescribeTopicPartitionsResponseTopic>
     */
    public array $topics = [];

    /**
     * First topic and partition of the page that follows, `null` when this answer completes the listing
     */
    public ?DescribeTopicPartitionsCursor $nextCursor = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'topics'         => ['name' => DescribeTopicPartitionsResponseTopic::class],
            'nextCursor'     => new NullableStruct(DescribeTopicPartitionsCursor::class),
        ];
    }
}
