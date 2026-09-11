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
 * ListGroups response, version 1 (key 16)
 *
 * <pre>
 *   ListGroups Response (Version: 1) => throttle_time_ms error_code [groups]
 *     throttle_time_ms => INT32     -- since version 1
 *     error_code       => INT16
 *     groups           => group_id protocol_type
 *       group_id      => STRING
 *       protocol_type => STRING
 * </pre>
 *
 * The error code belongs to the whole request: the coordinator answers 15 (GroupCoordinatorNotAvailable) while it is
 * shutting down and 14 (GroupLoadInProgress) while it is still reading the `__consumer_offsets` partitions it owns,
 * both with an empty group array (`GroupCoordinator.handleListGroups()` @ 0.11.0.3).
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the error code;
 * {@see ListGroupsResponseV0} is the answer without it.
 *
 * @see docs/protocol/2.8.md, sections "ListGroups API (key 16, v0 and v1)" and "Quotas and throttle time"
 */
class ListGroupsResponse extends AbstractResponse
{
    /**
     * Version of the ListGroups API that this class decodes the answer of
     */
    public const int VERSION = 1;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

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
        $body   = [];
        if (static::VERSION >= 1) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['errorCode'] = BinarySchema::TYPE_INT16;
        $body['groups']    = ['groupId' => ListGroupResponseProtocol::class];

        return $header + $body;
    }
}
