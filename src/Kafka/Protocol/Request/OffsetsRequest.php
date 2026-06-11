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
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;

/**
 * Offsets API
 *
 * This API describes the valid offset range available for a set of topic-partitions. As with the produce and fetch
 * APIs requests must be directed to the broker that is currently the leader for the partitions in question. This can
 * be determined using the metadata API.
 *
 * The response contains the starting offset of each segment for the requested partition as well as the "log end
 * offset" i.e. the offset of the next message that would be appended to the given partition.
 *
 * ListOffsets Request (Version: 1) => replica_id [topics]
 *   replica_id => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition timestamp
 *       partition => INT32
 *       timestamp => INT64
 */
class OffsetsRequest extends AbstractRequest
{
    /**
     * @inheritDoc
     */
    public const VERSION = 1;

    /**
     * Special value for the offset of the next coming message
     */
    public const LATEST = -1;

    /**
     * Special value for receiving the earliest available offset
     */
    public const EARLIEST = -2;

    private readonly array $topicPartitions;

    /**
     * @param int $replicaId
     */
    public function __construct(
        array $topicPartitions,
        /**
         * The replica id indicates the node id of the replica initiating this request. Normal client consumers should
         * always specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2
         * is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
         */
        private $replicaId = -1,
        $clientId = '',
        $correlationId = 0
    ) {
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopicPartitions[$topic] = new OffsetsRequestTopic($topic, $partitions);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(ApiKeys::OFFSETS, $clientId, $correlationId);
    }

    public static function getScheme()
    {
        $header = null;

        return $header + [
            'replicaId'       => BinarySchema::TYPE_INT32,
            'topicPartitions' => ['topic' => OffsetsRequestTopic::class],
        ];
    }
}
