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
 * One topic of an OffsetForLeaderEpoch request, version 2 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetForLeaderEpochRequestPartition
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v2)"
 */
class OffsetForLeaderEpochRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the OffsetForLeaderEpoch API that this DTO is packed for
     */
    public const int VERSION = 2;

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
     * @param string $topic Name of the topic
     * @param array<int, int|array{int, int}|OffsetForLeaderEpochRequestPartition> $partitionEpochs Epoch of every
     *        partition, indexed by the partition id; a value may be the pair `[leaderEpoch, currentLeaderEpoch]`
     *        that version 2 puts on the wire
     */
    public function __construct(string $topic, array $partitionEpochs)
    {
        $partitionClass = static::partitionClass();
        $partitions     = [];
        foreach ($partitionEpochs as $partition => $leaderEpoch) {
            if ($leaderEpoch instanceof OffsetForLeaderEpochRequestPartition) {
                $partitions[$partition] = $leaderEpoch;
                continue;
            }
            // A value is the epoch to look up, or the pair [leaderEpoch, currentLeaderEpoch] that version 2
            // (Kafka 2.1, KIP-320) puts on the wire
            [$epoch, $currentLeaderEpoch] = is_array($leaderEpoch)
                ? [(int) $leaderEpoch[0], (int) $leaderEpoch[1]]
                : [(int) $leaderEpoch, OffsetForLeaderEpochRequestPartition::UNKNOWN_LEADER_EPOCH];
            $partitions[$partition] = new $partitionClass((int) $partition, $epoch, $currentLeaderEpoch);
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
            'partitions' => ['partition' => static::partitionClass()],
        ];
    }

    /**
     * Returns the class of a partition entry for the version of the API that this DTO belongs to
     *
     * @return class-string<OffsetForLeaderEpochRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 2
            ? OffsetForLeaderEpochRequestPartition::class
            : OffsetForLeaderEpochRequestPartitionV0::class;
    }
}
