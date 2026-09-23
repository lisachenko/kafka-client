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
 * The partitions of one topic in a ConsumerGroupHeartbeat frame (key 68, Kafka 3.5, KIP-848)
 *
 * <pre>
 *   TopicPartitions => topic_id [partitions]
 *     topic_id   => UUID
 *     partitions => partition
 *       partition => INT32
 * </pre>
 *
 * `TopicPartitions` of `ConsumerGroupHeartbeatRequest.json` and the `commonStructs` entry of the same name of
 * `ConsumerGroupHeartbeatResponse.json` @ 3.9.2 - the **same structure on both sides** of the api, which is why
 * one class carries it: the member echoes the partitions it owns in its request, and the coordinator names the
 * ones it assigns in the `Assignment` of its answer.
 *
 * **The topic is named by its id and never by its name.** KIP-848 was written after the topic ids of KIP-516, so
 * the new consumer protocol knows nothing about topic names once the subscription has been resolved: a member
 * that receives an assignment has to turn the 16 raw bytes of each entry into a name through its metadata, and
 * refresh that metadata for an id it does not know yet ({@see \Protocol\Kafka\Common\Cluster::topicNameById()}).
 * That is the one difference to the classic protocol that a caller of this client can feel, because
 * {@see \Protocol\Kafka\Consumer\MemberAssignment} of the classic path names its topics.
 *
 * The array of partitions is an ordinary, non-nullable one on both sides; an entry with an empty array is legal
 * and means "this member owns no partition of that topic", which is what a member that has just revoked its last
 * partition of a topic sends while it still holds another one.
 *
 * @see \Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequest
 * @see \Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponse
 * @see docs/protocol/4.3.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 */
class ConsumerGroupHeartbeatTopicPartitions implements BinarySchemaInterface
{
    /**
     * @param string    $topicId    The 16 raw bytes of the topic id (KIP-516), never the name of the topic
     * @param list<int> $partitions Partitions of that topic, in the order they travel on the wire
     */
    public function __construct(
        /**
         * The 16 raw bytes of the id of the topic these partitions belong to
         */
        public string $topicId = '',
        /**
         * Partitions of that topic
         *
         * @var list<int>
         */
        public array $partitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}
