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
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponseGroup;
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponseGroupV0;

/**
 * The share-partition start offsets and lags of share groups (ApiKey 90, Kafka 4.1, KIP-932; v1 Kafka 4.2, KIP-1226)
 *
 * <pre>
 *   DescribeShareGroupOffsets Response (Version: 1) => ThrottleTimeMs [Groups] TAG_BUFFER
 *     ThrottleTimeMs => INT32
 *     Groups         => COMPACT_ARRAY of {@see DescribeShareGroupOffsetsResponseGroup}
 * </pre>
 *
 * There is **no top-level error code**: every group carries its own behind its topics, and every partition its
 * own behind its start offset. The supported errors of `DescribeShareGroupOffsetsResponse.json` @ 4.1.0 are the
 * authorization codes 30 and 29, the coordinator codes 14, 15 and 16, the **69** `GroupIdNotFound`, the 42 and
 * the -1.
 *
 * The version 1 of Kafka 4.2 adds the `lag` of KIP-1226 to every partition entry
 * ({@see \Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponsePartition::$lag}); the group and topic
 * entries are those of the version this class is unpacked from, and {@see DescribeShareGroupOffsetsResponseV0}
 * keeps the answer of the version 0.
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0 and v1)"
 * @see docs/protocol/4.3.md, section "The share-partition lag of KIP-1226 (v1)"
 */
class DescribeShareGroupOffsetsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every requested group, indexed by the group id
     *
     * @var array<string, DescribeShareGroupOffsetsResponseGroup>
     */
    public array $groups = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'groups'         => [
                'groupId' => static::VERSION >= 1
                    ? DescribeShareGroupOffsetsResponseGroup::class
                    : DescribeShareGroupOffsetsResponseGroupV0::class,
            ],
        ];
    }
}
