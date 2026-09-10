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
 * The result of committing the offsets of one topic inside a transaction, i.e. one entry of the `topics` array
 *
 * <pre>
 *   TxnOffsetCommitResponseTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => TxnOffsetCommitResponsePartition
 * </pre>
 *
 * @see docs/protocol/1.1.md, section "TxnOffsetCommit API (key 28, v0)"
 */
class TxnOffsetCommitResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this entry belongs to
     */
    public string $topic;

    /**
     * Result of every committed partition of this topic, indexed by the partition id
     *
     * @var array<int, TxnOffsetCommitResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => TxnOffsetCommitResponsePartition::class],
        ];
    }
}
