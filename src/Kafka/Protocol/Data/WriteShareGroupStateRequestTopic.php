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
 * One topic of a WriteShareGroupState request, named by its id (Kafka 4.1, KIP-932, v0)
 *
 * `WriteShareGroupStateRequestData.WriteStateData` @ 4.1.0: the topic id and the partitions of the share group
 * whose state is written.
 *
 * The topic of version 1 (Kafka 4.2, KIP-1226) carries its partitions as {@see WriteShareGroupStateRequestPartition},
 * with the `DeliveryCompleteCount` of that version; {@see WriteShareGroupStateRequestTopicV0} carries them as
 * {@see WriteShareGroupStateRequestPartitionV0}. Either converts the partitions it is given into the entry of its own
 * version, so a caller builds the partitions once and the class of the request decides what reaches the wire.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
class WriteShareGroupStateRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the WriteShareGroupState API that this DTO is packed into
     */
    public const int VERSION = 1;

    /**
     * The 16 raw bytes of the topic id
     */
    public string $topicId = '';

    /**
     * Partitions of this topic, indexed by the partition index
     *
     * @var array<int, WriteShareGroupStateRequestPartition>
     */
    public array $partitions = [];

    /**
     * @param string                                     $topicId    The 16 raw bytes of the topic id
     * @param list<WriteShareGroupStateRequestPartition> $partitions Partitions of this topic, indexed by the partition index
     */
    public function __construct(
        string $topicId = '',
        array $partitions = []
    ) {
        $this->topicId = $topicId;
        $partitionClass = static::partitionClass();
        foreach ($partitions as $item) {
            $this->partitions[$item->partition] = $item::class === $partitionClass
                ? $item
                : new $partitionClass(
                    $item->partition,
                    $item->stateEpoch,
                    $item->leaderEpoch,
                    $item->startOffset,
                    $item->stateBatches,
                    $item->deliveryCompleteCount
                );
        }
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => ['partition' => static::partitionClass()],
        ];
    }

    /**
     * Returns the class of a partition entry for the version of the API that this class is packed into
     *
     * @return class-string<WriteShareGroupStateRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 1
            ? WriteShareGroupStateRequestPartition::class
            : WriteShareGroupStateRequestPartitionV0::class;
    }
}
