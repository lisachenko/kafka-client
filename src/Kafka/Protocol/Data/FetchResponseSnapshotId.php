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
 * The snapshot a fetcher has to read instead of the log, the `SnapshotId` of a Fetch v12 answer
 *
 * The **tag 2** of a Fetch v12 partition entry (Kafka 2.7), and the one field of the three that belongs to the
 * **raft** replication of KIP-630 rather than to an ordinary consumer: a fetcher that asks for an offset below
 * the log start offset of a metadata partition is told the end offset and the epoch of the snapshot it should
 * fetch with a `FetchSnapshot` request (api key 59, which a ZooKeeper-backed broker does not serve at all).
 *
 * Both fields default to `-1`, and the structure is left out of every answer that has no snapshot to name - which
 * is every answer a 2.8.2 broker in ZooKeeper mode sends.
 *
 * @see docs/protocol/2.8.md, section "Epoch validation in the fetch itself (v12, KIP-595)"
 */
class FetchResponseSnapshotId implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO belongs to
     */
    public const int VERSION = 12;

    /**
     * Value of both fields when there is no snapshot to read
     */
    public const int UNDEFINED = -1;

    /**
     * Offset the snapshot ends at
     */
    public int $endOffset = self::UNDEFINED;

    /**
     * Epoch of that snapshot
     */
    public int $epoch = self::UNDEFINED;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'endOffset' => BinarySchema::TYPE_INT64,
            'epoch'     => BinarySchema::TYPE_INT32,
        ];
    }
}
