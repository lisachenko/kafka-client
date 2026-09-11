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
 * One member of a group with the metadata it joined with, as the JoinGroup response reports it.
 *
 * <pre>
 *   JoinGroupResponseMember => MemberId GroupInstanceId MemberMetadata
 *     MemberId        => string
 *     GroupInstanceId => nullable_string   -- since version 5
 *     MemberMetadata  => bytes
 * </pre>
 *
 * The coordinator fills this array **only in the answer it sends to the leader** of the group; every other member
 * receives an empty array (`GroupCoordinator.doJoinGroup` @ 0.10.2.2). The metadata is the one the member sent for
 * the protocol the coordinator selected, and it is opaque here.
 *
 * **Version 5 of the api (KIP-345, Kafka 2.3) added the `group_instance_id`**: the `group.instance.id` of a
 * static member, `null` for a dynamic one, which is what every member of a group of an older client is.
 * {@see JoinGroupResponseMemberV0} is the entry of the versions 0 to 4, which have no such field.
 *
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v5)"
 */
class JoinGroupResponseMember implements BinarySchemaInterface
{
    /**
     * Version of the JoinGroup API that this DTO decodes an entry of
     */
    public const int VERSION = 5;

    /**
     * Name of the group member.
     */
    public string $memberId;

    /**
     * `group.instance.id` of a static member (KIP-345), null for a dynamic one.
     *
     * @since Version 5 of protocol
     */
    public ?string $groupInstanceId = null;

    /**
     * Member-specific metadata of the selected protocol.
     */
    public string $metadata;

    public function __construct(string $memberId, string $metadata, ?string $groupInstanceId = null)
    {
        $this->memberId        = $memberId;
        $this->metadata        = $metadata;
        $this->groupInstanceId = $groupInstanceId;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = ['memberId' => BinarySchema::TYPE_STRING];
        if (static::VERSION >= 5) {
            $scheme['groupInstanceId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $scheme['metadata'] = BinarySchema::TYPE_BYTEARRAY;

        return $scheme;
    }
}
