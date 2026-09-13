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
 * SyncGroup request of version 2 (Kafka 2.0, KIP-219), the frame without the `group_instance_id` of version 3
 *
 * <pre>
 *   SyncGroup Request (Version: 0 to 2) => group_id generation_id member_id [group_assignment]
 * </pre>
 *
 * Version 3 (KIP-345, Kafka 2.3) inserted a nullable `group_instance_id` behind the member id, with which a
 * static member names itself; {@see SyncGroupRequest} sends that frame, this one is the version below it.
 *
 * @see docs/protocol/2.8.md, section "Static membership (KIP-345)"
 */
final class SyncGroupRequestV2 extends SyncGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
