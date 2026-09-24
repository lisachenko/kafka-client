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
 * One topic of a group in a DescribeShareGroupOffsets request (ApiKey 90, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DescribeShareGroupOffsetsRequestTopic => TopicName [Partitions] TAG_BUFFER
 *     TopicName  => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of INT32
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0 and v1)"
 */
final class DescribeShareGroupOffsetsRequestTopic implements BinarySchemaInterface
{
    /**
     * Partitions of the topic whose start offsets are asked for
     *
     * @var list<int>
     */
    public array $partitions;

    /**
     * @param string    $topicName  Name of the topic
     * @param list<int> $partitions Partitions to describe
     */
    public function __construct(
        /**
         * Name of the topic
         */
        public string $topicName,
        array $partitions
    ) {
        $this->partitions = array_values($partitions);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName'  => BinarySchema::TYPE_STRING,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}
