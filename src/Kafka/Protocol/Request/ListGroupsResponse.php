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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;

/**
 * List groups response
 *
 * <pre>
 *   ListGroupsResponse => ErrorCode [GroupId ProtocolType]
 *     ErrorCode    => int16
 *     GroupId      => string
 *     ProtocolType => string
 * </pre>
 *
 * The error code belongs to the whole request: the coordinator answers 15 (GroupCoordinatorNotAvailable) while it is
 * shutting down and 14 (GroupLoadInProgress) while it is still reading the `__consumer_offsets` partitions it owns,
 * both with an empty group array (`GroupCoordinator.handleListGroups()` @ 0.10.2.2).
 *
 * @see docs/protocol/0.10.2.md, section "ListGroups API (key 16, v0)"
 */
class ListGroupsResponse extends AbstractResponse
{
    /**
     * Error code of the whole request
     */
    public int $errorCode = 0;

    /**
     * Groups the answering broker coordinates, indexed by the group id
     *
     * @var array<string, ListGroupResponseProtocol>
     */
    public array $groups = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode' => BinarySchema::TYPE_INT16,
            'groups'    => ['groupId' => ListGroupResponseProtocol::class],
        ];
    }
}
