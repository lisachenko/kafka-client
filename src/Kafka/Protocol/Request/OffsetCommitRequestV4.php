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

namespace Protocol\Kafka\Protocol\Request;

/**
 * OffsetCommit request of version 4 (Kafka 2.0, KIP-219), the last version that carries `retention_time`
 *
 * <pre>
 *   OffsetCommit Request (Version: 2, 3 and 4) => group_id generation_id member_id retention_time [topics]
 * </pre>
 *
 * Version 5 (Kafka 2.1, KIP-211) **removes** `retention_time` from the frame, so this is the highest version a
 * client can ask for a retention of its own with; {@see OffsetCommitRequestV5} is the same request without the
 * field, and {@see OffsetCommitRequest} is version 6, which adds the leader epoch to every partition.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v7)"
 */
final class OffsetCommitRequestV4 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
