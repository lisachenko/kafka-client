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
 * The current leader of a partition in a ShareFetch or ShareAcknowledge answer (keys 78 and 79, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   CurrentLeader => leader_id leader_epoch
 * </pre>
 *
 * The `LeaderIdAndEpoch` of the two answers @ 4.1.0 - a plain field of every partition, not the tagged one of Fetch
 * v12 and later. `-1` is "unknown"; the 4.3.1 node fills it in on every partition it answers (leader 0, epoch 0 on
 * the one-node cluster).
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 */
final class ShareLeaderIdAndEpoch implements BinarySchemaInterface
{
    /**
     * Leader id or epoch that is not known
     */
    public const int UNKNOWN = -1;

    /**
     * Id of the current leader, -1 when it is not known
     */
    public int $leaderId = self::UNKNOWN;

    /**
     * Latest known leader epoch
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
