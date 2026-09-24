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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\Uuid;

/**
 * One topic with its partitions, as {@see AdminClient::describeTopicPartitions()} reports it
 *
 * `TopicDescription` of the Java admin client @ 3.9.2: the name, whether Kafka keeps the topic for itself, the
 * topic id of KIP-516, the partitions and the acl bit field of KIP-430. Where
 * {@see AdminClient::describeTopics()} answers the {@see \Protocol\Kafka\Common\TopicMetadata} of a **Metadata**
 * frame, this is the answer of DescribeTopicPartitions (key 75, Kafka 3.8), which adds the eligible leader
 * replicas of KIP-966 to every partition and is **paged**: the partitions of one description may have been
 * collected from several answers.
 *
 * The bit field is always filled here, because the api reports it without being asked - there is no
 * `include_topic_authorized_operations` flag in the request - so {@see self::authorizedOperations()} never
 * answers the empty list of an {@see AclOperation::NOT_REQUESTED}.
 *
 * @see docs/protocol/4.3.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
final class TopicDescription
{
    /**
     * @param string                      $name                 Name of the topic
     * @param bool                        $internal             Whether Kafka keeps the topic for itself
     * @param array<int, TopicPartitionInfo> $partitions        Partitions, indexed by the partition index
     * @param int                         $authorizedOperations Acl bit field of KIP-430 for this topic
     * @param string                      $topicId              The 16 raw bytes of the topic id (KIP-516)
     */
    public function __construct(
        public readonly string $name,
        public readonly bool $internal,
        public readonly array $partitions,
        public readonly int $authorizedOperations = AclOperation::NOT_REQUESTED,
        public readonly string $topicId = Uuid::ZERO
    ) {}

    /**
     * Returns the topic id the way a broker and `kafka-topics.sh --describe` print it
     */
    public function topicIdAsString(): string
    {
        return Uuid::toString($this->topicId);
    }

    /**
     * Returns the operations the principal of the connection may perform on this topic
     *
     * @return list<int>
     */
    public function authorizedOperations(): array
    {
        return AclOperation::fromBitField($this->authorizedOperations);
    }
}
