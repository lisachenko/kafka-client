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
 * LeaveGroup request of version 1 (Kafka 0.11), the frame of version 2 with a lower version field
 *
 * <pre>
 *   LeaveGroup Request (Version: 0, 1 and 2) => group_id member_id
 * </pre>
 *
 * Version 2 (KIP-219, Kafka 2.0) changed nothing on the wire; the api only changes again at version 3 (KIP-345,
 * Kafka 2.4), where the single `member_id` becomes a batch of member identities. The answer of version 1 is the
 * answer of version 2 and is read with {@see LeaveGroupResponseV1}.
 *
 * @see docs/protocol/2.8.md, sections "LeaveGroup API (key 13, v0 to v4)" and "Quotas and throttle time"
 */
final class LeaveGroupRequestV1 extends LeaveGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
