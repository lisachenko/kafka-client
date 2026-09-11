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
 *   JoinGroupResponseMember => MemberId MemberMetadata
 *     MemberId       => string
 *     MemberMetadata => bytes
 * </pre>
 *
 * The coordinator fills this array **only in the answer it sends to the leader** of the group; every other member
 * receives an empty array (`GroupCoordinator.doJoinGroup` @ 0.10.2.2). The metadata is the one the member sent for
 * the protocol the coordinator selected, and it is opaque here.
 *
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v3)"
 */
class JoinGroupResponseMember implements BinarySchemaInterface
{
    /**
     * Name of the group member.
     */
    public string $memberId;

    /**
     * Member-specific metadata of the selected protocol.
     */
    public string $metadata;

    public function __construct(string $memberId, string $metadata)
    {
        $this->memberId = $memberId;
        $this->metadata = $metadata;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId' => BinarySchema::TYPE_STRING,
            'metadata' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
