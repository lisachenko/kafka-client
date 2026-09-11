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
 * LeaveGroup request of version 2 (Kafka 2.0, KIP-219): the group id and the single member id
 *
 * <pre>
 *   LeaveGroup Request (Version: 0, 1 and 2) => group_id member_id
 * </pre>
 *
 * Version 3 (Kafka 2.4, KIP-345) replaced that member id with a **batch** of member identities, which is what
 * {@see LeaveGroupRequest} sends; this class is the last version of the single-member frame, and the highest one
 * a broker below Kafka 2.4 serves.
 *
 * @see docs/protocol/2.8.md, section "The batch leave of KIP-345 (v3)"
 */
final class LeaveGroupRequestV2 extends LeaveGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
