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
 * JoinGroup response, version 4: the answer of the versions 2 to 4, whose members carry no `group_instance_id`
 *
 * <pre>
 *   JoinGroup Response (Version: 2 to 4) => throttle_time_ms error_code generation_id group_protocol leader_id
 *                                           member_id [members]
 *     members => member_id member_metadata
 * </pre>
 *
 * Version 5 (KIP-345, Kafka 2.3) gave every entry of the member array a nullable `group_instance_id` behind its
 * member id, which {@see JoinGroupResponse} decodes; the answer of the versions 2 to 4 is this one.
 *
 * @see docs/protocol/2.8.md, section "Static membership (KIP-345)"
 */
final class JoinGroupResponseV4 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
