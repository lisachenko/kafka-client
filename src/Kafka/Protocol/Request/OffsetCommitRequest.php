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
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopic;

/**
 * OffsetCommit
 *
 * This api saves out the consumer's position in the stream for one or more partitions. In the scala API this happens
 * when the consumer calls commit() or in the background if "autocommit" is enabled. This is the position the consumer
 * will pick up from if it crashes before its next commit().
 *
 * OffsetCommit Request (Version: 2) => group_id generation_id member_id retention_time [topics]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   retention_time => INT64
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset metadata
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 */
class OffsetCommitRequest extends AbstractRequest
{
    /**
     * @inheritDoc
     */
    public const VERSION = 2;

    /**
     * Generation id for unsubscribed consumer
     */
    public const DEFAULT_GENERATION_ID = -1;

    /**
     * @var OffsetCommitRequestTopic[]
     */
    private readonly array $topicPartitions;

    /**
     * @param string $consumerGroup
     * @param int $generationId
     * @param string $memberName
     * @param int $retentionTime
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        private $consumerGroup,
        /**
         * The generation of the group.
         *
         * @since Version 1 of protocol
         */
        private $generationId,
        /**
         * The member id assigned by the group coordinator.
         *
         * @since Version 1 of protocol
         */
        private $memberName,
        /**
         * Time period in ms to retain the offset.
         *
         * @since Version 2 of protocol
         */
        private $retentionTime,
        array $topicPartitions,
        $clientId = '',
        $correlationId = 0
    ) {

        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopicPartitions[$topic] = new OffsetCommitRequestTopic($topic, $partitions);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(ApiKeys::OFFSET_COMMIT, $clientId, $correlationId);
    }

    public static function getScheme()
    {
        $header = null;

        return $header + [
            'consumerGroup'   => BinarySchema::TYPE_STRING,
            'generationId'    => BinarySchema::TYPE_INT32,
            'memberName'      => BinarySchema::TYPE_STRING,
            'retentionTime'   => BinarySchema::TYPE_INT64,
            'topicPartitions' => ['topic' => OffsetCommitRequestTopic::class],
        ];
    }
}
