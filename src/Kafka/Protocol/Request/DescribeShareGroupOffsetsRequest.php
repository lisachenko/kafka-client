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
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsRequestGroup;

/**
 * Reads the share-partition start offsets and lags of share groups (ApiKey 90, Kafka 4.1, KIP-932; v1 KIP-1226)
 *
 * <pre>
 *   DescribeShareGroupOffsets Request (Version: 1) => [Groups] TAG_BUFFER
 *     Groups => COMPACT_ARRAY of {@see DescribeShareGroupOffsetsRequestGroup}
 * </pre>
 *
 * The admin half of the share groups of KIP-932: what `kafka-share-groups.sh --describe --offsets` and the Java
 * `Admin.listShareGroupOffsets()` ask. A share group has no committed offset per member - its members acknowledge
 * records one by one - so the one number per partition that describes where the group stands is the
 * **share-partition start offset**, the first offset the group is not done with, which the share coordinator keeps
 * in `__share_group_state`. The request goes to the **group coordinator** of every group, found with
 * FindCoordinator and the key type 0 like a consumer group, and asks for the partitions of each group - or,
 * with a null topic array, for every partition the group holds state for.
 *
 * **Kafka 4.1 added the api** (`DescribeShareGroupOffsetsRequest.json` @ 4.1.0, `"validVersions": "0"`,
 * flexible from its version 0, a `broker` listener api). **Kafka 4.2 added the version 1** (`"validVersions":
 * "0-1"` @ 4.2.0, *"Version 1 introduces Lag in the response (KIP-1226)"*): the request is the frame of the version
 * 0, and the answer carries the share-partition `lag` of every partition. {@see DescribeShareGroupOffsetsRequestV0}
 * keeps the version 0.
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0 and v1)"
 */
class DescribeShareGroupOffsetsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Groups to describe, indexed by the group id
     *
     * @var array<string, DescribeShareGroupOffsetsRequestGroup>
     */
    protected readonly array $groups;

    /**
     * @param array<string, array<string, list<int>>|null> $groups Partitions to describe per group, as
     *        group => topic => partitions; `null` for every topic-partition that group holds state for
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(array $groups, string $clientId = '', int $correlationId = 0)
    {
        $packed = [];
        foreach ($groups as $groupId => $topics) {
            $packed[(string) $groupId] = new DescribeShareGroupOffsetsRequestGroup((string) $groupId, $topics);
        }
        $this->groups = $packed;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groups' => ['groupId' => DescribeShareGroupOffsetsRequestGroup::class],
        ];
    }

    /**
     * Returns the groups this request asks for, indexed by the group id
     *
     * @return array<string, DescribeShareGroupOffsetsRequestGroup>
     */
    public function getGroups(): array
    {
        return $this->groups;
    }
}
