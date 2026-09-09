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
 * One topic of a DeleteRecords request, i.e. one entry of the `topics` array
 *
 * <pre>
 *   DeleteRecordsRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => DeleteRecordsRequestPartition
 * </pre>
 *
 * `DELETE_RECORDS_REQUEST_TOPIC_V0` in `Protocol.java` @ 0.11.0.3. The Java client keeps the request as a flat
 * `Map<TopicPartition, Long>` and groups it by topic only while it writes the frame
 * (`CollectionUtils.groupDataByTopic`), which is exactly the shape of this DTO.
 *
 * @see docs/protocol/0.11.0.md, section "DeleteRecords API (key 21, v0)"
 */
class DeleteRecordsRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic to delete records from
     */
    public string $topic;

    /**
     * Partitions of this topic, indexed by the partition id
     *
     * @var array<int, DeleteRecordsRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer value is the offset to delete before, an already built partition DTO is taken as it is.
     *
     * @param string                                       $topic            Name of the topic
     * @param array<int, int|DeleteRecordsRequestPartition> $partitionOffsets Offset of every partition, indexed by
     *        the partition id
     */
    public function __construct(string $topic, array $partitionOffsets)
    {
        $partitions = [];
        foreach ($partitionOffsets as $partition => $offset) {
            $partitions[$partition] = $offset instanceof DeleteRecordsRequestPartition
                ? $offset
                : new DeleteRecordsRequestPartition((int) $partition, $offset);
        }

        $this->topic      = $topic;
        $this->partitions = $partitions;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => DeleteRecordsRequestPartition::class],
        ];
    }
}
