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

/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * This API describes the valid offset range available for a set of topic-partitions.
 *
 * As with the produce and fetch APIs requests must be directed to the broker that is currently the leader for the
 * partitions in question. This can be determined using the metadata API.
 *
 * The response contains the starting offset of each segment for the requested partition as well as the "log end
 * offset" i.e. the offset of the next message that would be appended to the given partition.
 *
 * Since v2 if no topics (null input for list of topics) are provided, the offset information of all topics (or topic
 * partitions) associated with the group is returned
 *
 * OffsetFetch Request (Version: 2) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 */
class OffsetFetchRequest extends AbstractRequest
{
    /**
     * @inheritDoc
     */
    public const VERSION = 2;

    /**
     * OffsetFetchRequest constructor.
     *
     * @param string            $consumerGroup   Name of the consumer group
     * @param PartitionsForTopic[] $topicPartitions List of topic => partitions to fetch
     * @param string            $clientId        Unique client identifier
     * @param int               $correlationId   Correlated request ID
     */
    public function __construct(/**
     * The consumer group id.
     */
        protected $consumerGroup,
        protected ?array $topicPartitions = null,
        $clientId = '',
        $correlationId = 0
    ) {
        parent::__construct(ApiKeys::OFFSET_FETCH, $clientId, $correlationId);
    }

    public static function getScheme()
    {
        $header = null;

        return $header + [
            'consumerGroup'   => BinarySchema::TYPE_STRING,
            'topicPartitions' => ['topic' => PartitionsForTopic::class, BinarySchema::FLAG_NULLABLE => true],
        ];
    }
}
