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

use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a TxnOffsetCommit request, i.e. one entry of the `topics` array
 *
 * <pre>
 *   TxnOffsetCommitRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => TxnOffsetCommitRequestPartition
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v2)"
 */
class TxnOffsetCommitRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the TxnOffsetCommit API that this DTO belongs to
     */
    public const int VERSION = 2;

    /**
     * Name of the topic whose offsets are committed
     */
    public string $topic;

    /**
     * Offsets of every partition of this topic, indexed by the partition id
     *
     * @var array<int, TxnOffsetCommitRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer or an {@see OffsetAndMetadata} becomes a partition DTO, an already built one is kept
     *
     * @param string $topic Name of the topic
     * @param array<int, int|OffsetAndMetadata|TxnOffsetCommitRequestPartition> $partitionOffsets Offset of every
     *        partition, indexed by the partition id
     */
    public function __construct(string $topic, array $partitionOffsets)
    {
        $partitionClass = static::partitionClass();
        $partitions     = [];
        foreach ($partitionOffsets as $partition => $offset) {
            $partitions[$partition] = $offset instanceof TxnOffsetCommitRequestPartition
                ? $offset
                : new $partitionClass((int) $partition, $offset);
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
     * @return class-string<TxnOffsetCommitRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 2
            ? TxnOffsetCommitRequestPartition::class
            : TxnOffsetCommitRequestPartitionV0::class;
    }
}
