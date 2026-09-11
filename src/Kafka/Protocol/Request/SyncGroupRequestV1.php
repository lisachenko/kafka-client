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
 * SyncGroup request of version 1 (Kafka 0.11), the frame of version 2 with a lower version field
 *
 * <pre>
 *   SyncGroup Request (Version: 0, 1 and 2) => group_id generation_id member_id [group_assignment]
 * </pre>
 *
 * Version 2 (KIP-219, Kafka 2.0) added no field - the next one is the `group_instance_id` of version 3 (KIP-345,
 * Kafka 2.3) - so this class sends the very same bytes as {@see SyncGroupRequest} and reads its answer with
 * {@see SyncGroupResponseV1}.
 *
 * @see docs/protocol/2.8.md, sections "SyncGroup API (key 14, v0 to v5)" and "Quotas and throttle time"
 */
final class SyncGroupRequestV1 extends SyncGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
