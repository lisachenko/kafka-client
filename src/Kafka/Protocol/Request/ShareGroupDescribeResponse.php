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
use Protocol\Kafka\Protocol\Data\ShareGroupDescribedGroup;

/**
 * ShareGroupDescribe response, version 1 (key 77, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ShareGroupDescribe Response (Version: 1) => throttle_time_ms [groups]
 *     groups => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name [members]
 *               authorized_operations
 * </pre>
 *
 * There is no top-level error code: every group carries its own ({@see ShareGroupDescribedGroup}).
 *
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
class ShareGroupDescribeResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * The api is flexible from its first version
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Described groups, indexed by the group id
     *
     * @var array<string, ShareGroupDescribedGroup>
     */
    public array $groups = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return parent::getScheme() + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'groups'         => ['groupId' => ShareGroupDescribedGroup::class],
        ];
    }
}
