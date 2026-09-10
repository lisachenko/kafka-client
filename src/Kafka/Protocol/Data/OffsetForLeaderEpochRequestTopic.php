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
 * One topic of an OffsetForLeaderEpoch request, version 0 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetForLeaderEpochRequestPartition
 * </pre>
 *
 * @see docs/protocol/1.1.md, section "OffsetForLeaderEpoch API (key 23, v0)"
 */
class OffsetForLeaderEpochRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $topic;

    /**
     * Epoch asked for per partition, indexed by the partition id
     *
     * @var array<int, OffsetForLeaderEpochRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer value is the leader epoch of the partition, an already built partition DTO is taken as it is.
     *
     * @param string                                                   $topic          Name of the topic
     * @param array<int, int|OffsetForLeaderEpochRequestPartition>     $partitionEpochs Epoch of every partition,
     *        indexed by the partition id
     */
    public function __construct(string $topic, array $partitionEpochs)
    {
        $partitions = [];
        foreach ($partitionEpochs as $partition => $leaderEpoch) {
            $partitions[$partition] = $leaderEpoch instanceof OffsetForLeaderEpochRequestPartition
                ? $leaderEpoch
                : new OffsetForLeaderEpochRequestPartition((int) $partition, $leaderEpoch);
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
            'partitions' => ['partition' => OffsetForLeaderEpochRequestPartition::class],
        ];
    }
}
