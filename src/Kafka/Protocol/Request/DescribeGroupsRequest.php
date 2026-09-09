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
 *   DescribeGroups Request (Version: 0 and 1) => [group_ids]
 *     group_ids => STRING
 * </pre>
 *
 * `DESCRIBE_GROUPS_REQUEST_V1 = DESCRIBE_GROUPS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124,
 * Kafka 0.11) added the `throttle_time_ms` to the ANSWER alone ({@see DescribeGroupsResponse}), so
 * {@see DescribeGroupsRequestV0} puts the same bytes on the wire.
 *
 * @see docs/protocol/0.11.0.md, section "DescribeGroups API (key 15, v0 and v1)"
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
    public const int VERSION = 1;

    /**
     * @param list<string> $groups        Groups to describe, an empty list is answered with an empty description
     * @param string       $clientId      A user specified identifier for the client making the request
     * @param int          $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        protected array $groups,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groups' => [BinarySchema::TYPE_STRING],
        ];
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
}
