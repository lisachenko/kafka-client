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
 * The leader a partition really has, the `LeaderIdAndEpoch` of a Fetch v12 answer
 *
 * The **tag 1** of a Fetch v12 partition entry (Kafka 2.7). A broker that refuses a partition because the fetcher
 * asked the wrong node - or with a stale epoch - can name the node and the epoch it should ask instead, so that
 * the fetcher goes to the right broker without a Metadata round trip. Both fields default to `-1`, "unknown",
 * and the structure is left out of an answer that has nothing to say, which is what a tagged field is for.
 *
 * @see docs/protocol/2.8.md, section "Epoch validation in the fetch itself (v12, KIP-595)"
 */
class FetchResponseCurrentLeader implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO belongs to
     */
    public const int VERSION = 12;

    /**
     * Value of both fields when the broker does not know the leader
     */
    public const int UNKNOWN = -1;

    /**
     * Node id of the current leader of the partition
     */
    public int $leaderId = self::UNKNOWN;

    /**
     * Latest leader epoch the broker knows for the partition
     */
    public int $leaderEpoch = self::UNKNOWN;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'leaderId'    => BinarySchema::TYPE_INT32,
            'leaderEpoch' => BinarySchema::TYPE_INT32,
        ];
    }
}
