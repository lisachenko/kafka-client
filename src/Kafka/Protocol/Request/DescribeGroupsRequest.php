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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * This API describes the groups that the broker it is sent to is the coordinator of.
 *
 * A group has to be asked from its own coordinator ({@see GroupCoordinatorRequest}): another broker answers the
 * group with the error code 16 (NotCoordinatorForGroup) and an empty description. An empty group array is legal and
 * is answered with an empty group array.
 *
 * <pre>
 *   DescribeGroups Request (Version: 0 to 3) => [group_ids] include_authorized_operations
 *     group_ids                     => STRING
 *     include_authorized_operations => BOOLEAN   -- since version 3
 * </pre>
 *
 * `DESCRIBE_GROUPS_REQUEST_V1 = DESCRIBE_GROUPS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124,
 * Kafka 0.11) added the `throttle_time_ms` to the ANSWER alone ({@see DescribeGroupsResponse}), so
 * {@see DescribeGroupsRequestV0} puts the same bytes on the wire. Version 2 (KIP-219, Kafka 2.0) added nothing
 * either and {@see DescribeGroupsRequestV1} sends the same frame. **Version 3 (KIP-430, Kafka 2.3) appended the
 * boolean `include_authorized_operations`**, the one field the request has ever gained: a `true` asks the broker
 * to report, per group, the operations the client that sends the request may perform on it
 * ({@see \Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata::$authorizedOperations}), and a `false`
 * leaves that bit set at {@see \Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED}.
 * {@see DescribeGroupsRequestV2} is the frame without the flag.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
class DescribeGroupsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_GROUPS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 5;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 5;

    /**
     * @param list<string> $groups        Groups to describe, an empty list is answered with an empty description
     * @param string       $clientId      A user specified identifier for the client making the request
     * @param int          $correlationId A user-supplied value that the broker passes back unmodified
     * @param bool         $includeAuthorizedOperations Whether the answer reports the operations this client may
     *        perform on each group (KIP-430), ignored by every version below 3
     */
    public function __construct(
        protected array $groups,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Whether the broker reports the authorized operations of every described group.
         *
         * @since Version 3 of protocol
         */
        protected readonly bool $includeAuthorizedOperations = false
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        $body = ['groups' => [BinarySchema::TYPE_STRING]];
        if (static::VERSION >= 3) {
            $body['includeAuthorizedOperations'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }

    /**
     * Returns the groups this request asks the description of
     *
     * @return list<string>
     */
    public function getGroups(): array
    {
        return $this->groups;
    }

    /**
     * Tells whether this request asks for the operations the client may perform on each group (KIP-430)
     */
    public function includesAuthorizedOperations(): bool
    {
        return $this->includeAuthorizedOperations;
    }
}
