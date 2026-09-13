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
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadataV0;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadataV3;

/**
 * DescribeGroups response, version 3 (key 15)
 *
 * <pre>
 *   DescribeGroups Response (Version: 1 to 3) => throttle_time_ms [groups]
 *     throttle_time_ms => INT32     -- since version 1
 *     groups           => error_code group_id state protocol_type protocol [members] authorized_operations
 *       authorized_operations => INT32   -- since version 3
 * </pre>
 *
 * There is no error code for the request as a whole: every group carries its own, and the order of the array is the
 * order of the group ids in the request.
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the array and left the entries alone -
 * `DESCRIBE_GROUPS_RESPONSE_V1` reuses `DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0` in `Protocol.java` @ 0.11.0.3;
 * {@see DescribeGroupsResponseV0} is the answer without it. Version 2 (KIP-219, Kafka 2.0) repeats the version 1
 * answer, which {@see DescribeGroupsResponseV1} decodes. **Version 3 (KIP-430, Kafka 2.3) appended an
 * `authorized_operations` bit set to every group entry**, which is what
 * {@see \Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata::$authorizedOperations} carries;
 * {@see DescribeGroupsResponseV2} is the answer whose entries end with the member array. The entry grows once
 * more at version 4 (KIP-345, Kafka 2.4), which gives every member a `group_instance_id`.
 *
 * @see docs/protocol/2.8.md, sections "DescribeGroups API (key 15, v0 to v5)" and "Quotas and throttle time"
 */
class DescribeGroupsResponse extends AbstractResponse
{
    /**
     * Version of the DescribeGroups API that this class decodes the answer of
     */
    public const int VERSION = 5;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 5;

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
        $body['groups'] = ['groupId' => static::groupClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a group entry for the version of the API that this class decodes
     *
     * @return class-string<DescribeGroupResponseMetadata>
     */
    protected static function groupClass(): string
    {
        return match (true) {
            static::VERSION >= 4  => DescribeGroupResponseMetadata::class,
            static::VERSION === 3 => DescribeGroupResponseMetadataV3::class,
            default               => DescribeGroupResponseMetadataV0::class,
        };
    }
}
