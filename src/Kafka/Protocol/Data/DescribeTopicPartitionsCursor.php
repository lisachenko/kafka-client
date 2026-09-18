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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The page marker of DescribeTopicPartitions (key 75, Kafka 3.8, KIP-966)
 *
 * <pre>
 *   Cursor => TopicName PartitionIndex
 *     TopicName      => COMPACT_STRING
 *     PartitionIndex => INT32
 * </pre>
 *
 * `Cursor` of `DescribeTopicPartitionsRequest.json` and of `DescribeTopicPartitionsResponse.json` @ 3.8.1 - the
 * **same structure on both sides** of the api, which is why one class carries it: the answer ends a page with the
 * `next_cursor` the next request puts into its `cursor` field. Both fields are
 * {@see \Protocol\Kafka\Protocol\NullableStruct} fields, and a `null` one means "start at the beginning" in the
 * request and "there is nothing left" in the answer.
 *
 * The marker is read as "**the first** topic and partition of the page that follows", never as the last one that
 * was delivered: `KRaftMetadataCache.getTopicMetadataForDescribeTopicResponse` @ 3.9.2 begins the topic named
 * here at this partition index and every topic behind it at its partition 0.
 *
 * @see docs/protocol/3.9.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
class DescribeTopicPartitionsCursor implements BinarySchemaInterface
{
    /**
     * @param string $topicName      First topic of the page, which the request has to name in its topic array
     * @param int    $partitionIndex First partition of that topic to describe, never negative
     */
    public function __construct(
        /**
         * Name of the first topic the page starts at
         */
        public string $topicName = '',
        /**
         * Index of the first partition of that topic the page starts at
         */
        public int $partitionIndex = 0
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName'      => BinarySchema::TYPE_STRING,
            'partitionIndex' => BinarySchema::TYPE_INT32,
        ];
    }
}
