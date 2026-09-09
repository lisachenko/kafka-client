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
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;

/**
 * DescribeGroups response, version 1 (key 15)
 *
 * <pre>
 *   DescribeGroups Response (Version: 1) => throttle_time_ms [groups]
 *     throttle_time_ms => INT32     -- since version 1
 *     groups           => error_code group_id state protocol_type protocol [members]
 * </pre>
 *
 * There is no error code for the request as a whole: every group carries its own, and the order of the array is the
 * order of the group ids in the request.
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the array and left the entries alone -
 * `DESCRIBE_GROUPS_RESPONSE_V1` reuses `DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0` in `Protocol.java` @ 0.11.0.3;
 * {@see DescribeGroupsResponseV0} is the answer without it.
 *
 * @see docs/protocol/0.11.0.md, sections "DescribeGroups API (key 15, v0 and v1)" and "Quotas and throttle time"
 */
class DescribeGroupsResponse extends AbstractResponse
{
    /**
     * Version of the DescribeGroups API that this class decodes the answer of
     */
    public const int VERSION = 1;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Description of each requested group, indexed by the group id
     *
     * @var array<string, DescribeGroupResponseMetadata>
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
        $body['groups'] = ['groupId' => DescribeGroupResponseMetadata::class];

        return $header + $body;
    }
}
