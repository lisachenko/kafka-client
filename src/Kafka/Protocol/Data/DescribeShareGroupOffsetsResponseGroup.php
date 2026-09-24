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
 * One share group of a DescribeShareGroupOffsets answer (ApiKey 90, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DescribeShareGroupOffsetsResponseGroup => GroupId [Topics] ErrorCode ErrorMessage TAG_BUFFER
 *     GroupId      => COMPACT_STRING
 *     Topics       => COMPACT_ARRAY of {@see DescribeShareGroupOffsetsResponseTopic}
 *     ErrorCode    => INT16
 *     ErrorMessage => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * The errors of a group come **after** its topics, and a group that could not be described carries an empty topic
 * array next to its code - the **69** `GroupIdNotFound` of a group the coordinator does not hold, among others.
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0)"
 */
final class DescribeShareGroupOffsetsResponseGroup implements BinarySchemaInterface
{
    /**
     * Id of the share group
     */
    public string $groupId;

    /**
     * Result of every topic, indexed by the topic name
     *
     * @var array<string, DescribeShareGroupOffsetsResponseTopic>
     */
    public array $topics = [];

    /**
     * Error of the group, 0 when it could be described
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId'      => BinarySchema::TYPE_STRING,
            'topics'       => ['topicName' => DescribeShareGroupOffsetsResponseTopic::class],
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
