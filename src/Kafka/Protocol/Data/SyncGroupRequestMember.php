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
 * The assignment that the leader of a group computed for one member, as a SyncGroup request carries it.
 *
 * <pre>
 *   SyncGroupRequestMember => MemberId MemberAssignment
 *     MemberId         => string
 *     MemberAssignment => bytes
 * </pre>
 *
 * Only the leader of the group fills this array; every other member sends an empty one and just picks its own
 * assignment out of the response. The coordinator stores the bytes and hands each member its own, adding an empty
 * assignment for a member the leader did not mention (`GroupCoordinator.doSyncGroup` @ 0.10.2.2) - it never parses
 * them, which is why the assignment is an opaque byte array here as well.
 *
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 to v4)"
 */
class SyncGroupRequestMember implements BinarySchemaInterface
{
    /**
     * Name of the group member this assignment is meant for.
     */
    public string $memberId;

    /**
     * Member-specific assignment, opaque to the coordinator.
     */
    public string $assignment;

    public function __construct(string $memberId, string $assignment)
    {
        $this->memberId   = $memberId;
        $this->assignment = $assignment;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId'   => BinarySchema::TYPE_STRING,
            'assignment' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
