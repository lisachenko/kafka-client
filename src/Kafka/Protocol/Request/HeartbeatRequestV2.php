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
 * Heartbeat request of version 2 (Kafka 2.0, KIP-219), the frame without the `group_instance_id` of version 3
 *
 * <pre>
 *   Heartbeat Request (Version: 0 to 2) => group_id group_generation_id member_id
 * </pre>
 *
 * Version 3 (KIP-345, Kafka 2.3) appended a nullable `group_instance_id`, with which a static member names
 * itself and which lets the coordinator fence an older instance of it with the error code 82
 * (`FencedInstanceId`); {@see HeartbeatRequest} sends that frame.
 *
 * @see docs/protocol/2.8.md, section "Static membership (KIP-345)"
 */
final class HeartbeatRequestV2 extends HeartbeatRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
