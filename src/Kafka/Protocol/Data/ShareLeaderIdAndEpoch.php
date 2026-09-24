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
 * v12 and later, with no default in the spec, so it is `0 0` on the wire unless the node fills it in - and the 4.3.1
 * node does that only for a partition it answers 6 `NotLeaderOrFollower` or 74 `FencedLeaderEpoch`, together with
 * the endpoint of the new leader in `node_endpoints` (`KafkaApis.processShareFetchResponse` and
 * `processShareAcknowledgeResponse` @ 4.3.1). `-1` is "unknown" there.
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
     * Id of the current leader, -1 when it is not known, 0 when the node did not fill it in
     */
    public int $leaderId = 0;

    /**
     * Latest known leader epoch, 0 when the node did not fill it in
     */
    public int $leaderEpoch = 0;

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
