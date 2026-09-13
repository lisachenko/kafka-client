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
 * One topic of an Offsets (ListOffset) request, version 1
 *
 * <pre>
 *   OffsetsRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetsRequestPartition
 * </pre>
 *
 * The topic entry itself is the same in every version of the request; only the layout of a partition entry
 * changes, so the class of the entries is derived from {@see OffsetsRequestTopic::VERSION}, which
 * {@see OffsetsRequestTopicV1} and {@see OffsetsRequestTopicV0} lower. The **encoding** of the entry is not its
 * own business: a version 6 request (Kafka 2.8, KIP-482) writes this very scheme with compact types and a
 * tagged-field section, which the engine derives from the top-level message, see {@see BinarySchema}.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v6), a.k.a. ListOffset"
 */
class OffsetsRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Offsets API that this DTO is packed for
     */
    public const int VERSION = 4;

    /**
     * Name of the topic to list the offsets of
     */
    public string $topic;

    /**
     * Partitions of this topic to list the offsets of, indexed by the partition id
     *
     * @var array<int, OffsetsRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer value is the target time of the partition, an already built partition DTO is taken as it is.
     *
     * @param string $topic Name of the topic
     * @param array<int, int|OffsetsRequestPartition> $partitionTimestamps Target time for each partition, indexed
     *                                                                    by the partition id
     * @param int    $maxNumberOfOffsets Offsets to return per partition, version 0 of the api only
     */
    public function __construct(string $topic, array $partitionTimestamps, int $maxNumberOfOffsets = 1)
    {
        $partitionClass = static::partitionClass();
        $partitions     = [];
        foreach ($partitionTimestamps as $partition => $timestamp) {
            if ($timestamp instanceof OffsetsRequestPartition) {
                $partitions[$partition] = $timestamp;
                continue;
            }
            // A value may be the plain target timestamp, or the pair [timestamp, currentLeaderEpoch] that
            // version 4 (Kafka 2.1, KIP-320) puts on the wire
            [$targetTime, $currentLeaderEpoch] = is_array($timestamp)
                ? [(int) $timestamp[0], (int) $timestamp[1]]
                : [(int) $timestamp, OffsetsRequestPartition::UNKNOWN_LEADER_EPOCH];
            $partitions[$partition] = new $partitionClass(
                (int) $partition,
                $targetTime,
                $maxNumberOfOffsets,
                $currentLeaderEpoch
            );
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
     * Returns the class of a partition entry for the version of the API that this class packs
     *
     * @return class-string<OffsetsRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 4 => OffsetsRequestPartition::class,
            static::VERSION >= 1 => OffsetsRequestPartitionV1::class,
            default              => OffsetsRequestPartitionV0::class,
        };
    }
}
