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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * OffsetFetch, version 1: the offsets are read from the `__consumer_offsets` topic of the cluster.
 *
 * This API reads back the offsets that were committed for a consumer group with the OffsetCommit API, so it has to
 * be sent to the coordinator of that group.
 *
 * <pre>
 *   OffsetFetch Request (Version: 0 and 1) => group_id [topics]
 *     group_id => STRING
 *     topics   => topic [partitions]
 *       topic      => STRING
 *       partitions => partition
 *         partition => INT32
 * </pre>
 *
 * Both versions are identical on the wire and only differ in where the broker reads the offsets from, therefore
 * {@see OffsetFetchRequestV0} only lowers the version constant. The nullable topic array, which asks the coordinator
 * for every topic of the group, arrived with version 2 in Kafka 0.9 and does not exist here.
 *
 * @see docs/protocol/0.10.2.md, section "OffsetFetch API (key 9, v0 and v1)"
 */
class OffsetFetchRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Partitions whose offsets are requested, indexed by the topic they belong to.
     *
     * @var array<string, PartitionsForTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * @param string $consumerGroup   Name of the consumer group
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Partitions to fetch, per topic
     * @param string $clientId        Unique client identifier
     * @param int    $correlationId   Correlated request id
     */
    public function __construct(
        protected readonly string $consumerGroup,
        array $topicPartitions,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopicPartitions[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values($partitions));
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(ApiKeys::OFFSET_FETCH, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'consumerGroup'   => BinarySchema::TYPE_STRING,
            'topicPartitions' => ['topic' => PartitionsForTopic::class],
        ];
    }
}
