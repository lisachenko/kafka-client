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

use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a DescribeTopicPartitions answer (key 75, Kafka 3.8, KIP-966)
 *
 * <pre>
 *   DescribeTopicPartitionsResponseTopic => ErrorCode Name TopicId IsInternal [Partitions]
 *                                           TopicAuthorizedOperations
 *     ErrorCode                 => INT16
 *     Name                      => COMPACT_NULLABLE_STRING
 *     TopicId                   => UUID (16 raw bytes)
 *     IsInternal                => BOOLEAN
 *     Partitions                => COMPACT_ARRAY of {@see DescribeTopicPartitionsResponsePartition}
 *     TopicAuthorizedOperations => INT32
 * </pre>
 *
 * `DescribeTopicPartitionsResponseTopic` of `DescribeTopicPartitionsResponse.json` @ 3.8.1. The entry is the topic
 * half of a Metadata answer plus the acl bit field of KIP-430, which this api reports **without being asked**: the
 * request has no `include_topic_authorized_operations` flag, `DescribeTopicPartitionsRequestHandler` @ 3.9.2 fills
 * the field for every topic it answers, and the value is therefore the bit field of the principal, never the
 * {@see AclOperation::NOT_REQUESTED} of an answer that was not asked.
 *
 * The `name` is nullable in the specification, as in a Metadata v12 entry, but this api is asked by name: the node
 * fills it in for every entry, the refused ones and the unknown ones included. A topic the cluster does not host
 * is the error code **3** with the zero topic id and no partition; a name that is not a legal topic name is **17**
 * (`InvalidTopic`), and a topic the principal may not `Describe` is **29** with the zero id.
 *
 * @see docs/protocol/4.3.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
class DescribeTopicPartitionsResponseTopic implements BinarySchemaInterface
{
    /**
     * Error of this topic, 0 when it could be described
     */
    public int $errorCode = 0;

    /**
     * Name of the topic, `null` for an entry the node could not name
     */
    public ?string $name = null;

    /**
     * Id of the topic, the 16 raw bytes of the `uuid` of KIP-516, {@see Uuid::ZERO} for an entry without one
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Whether Kafka keeps this topic for itself (`__consumer_offsets`, `__transaction_state`)
     */
    public bool $isInternal = false;

    /**
     * Partitions of this topic that the page carries, indexed by the partition index
     *
     * @var array<int, DescribeTopicPartitionsResponsePartition>
     */
    public array $partitions = [];

    /**
     * Operations the principal of this connection is authorized for on this topic, as the bit field of KIP-430
     */
    public int $topicAuthorizedOperations = AclOperation::NOT_REQUESTED;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'                 => BinarySchema::TYPE_INT16,
            'name'                      => BinarySchema::TYPE_NULLABLE_STRING,
            'topicId'                   => BinarySchema::TYPE_UUID,
            'isInternal'                => BinarySchema::TYPE_BOOLEAN,
            'partitions'                => ['partitionIndex' => DescribeTopicPartitionsResponsePartition::class],
            'topicAuthorizedOperations' => BinarySchema::TYPE_INT32,
        ];
    }
}
