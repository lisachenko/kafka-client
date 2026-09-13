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
 * OffsetCommit request of version 6 (Kafka 2.1, KIP-320), the frame without the `group_instance_id` of version 7
 *
 * <pre>
 *   OffsetCommit Request (Version: 6) => group_id generation_id member_id [topics]
 *     topics => topic [partitions]
 *       partitions => partition offset leader_epoch metadata
 * </pre>
 *
 * Version 7 (KIP-345, Kafka 2.3) inserted a nullable `group_instance_id` behind the member id, so that a static
 * member commits under the identity its `group.instance.id` gives it; {@see OffsetCommitRequest} sends that
 * frame, this one is the version below it and the last one a client without static membership needs.
 *
 * @see docs/protocol/2.8.md, section "Static membership (KIP-345)"
 */
final class OffsetCommitRequestV6 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
