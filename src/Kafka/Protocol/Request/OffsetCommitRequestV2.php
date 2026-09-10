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
 * OffsetCommit request of version 2 (Kafka 0.9), the frame of version 3 with a lower version field
 *
 * <pre>
 *   OffsetCommit Request (Version: 2) => group_id generation_id member_id retention_time [topics]
 * </pre>
 *
 * `OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2` in `Protocol.java` @ 0.11.0.3, so this class sends the very
 * same body as {@see OffsetCommitRequest}; the answer of version 3 gained the leading `throttle_time_ms` and is
 * therefore read with {@see OffsetCommitResponseV2} for this version.
 *
 * @see docs/protocol/1.1.md, section "OffsetCommit API (key 8, v0 to v3)"
 */
final class OffsetCommitRequestV2 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
