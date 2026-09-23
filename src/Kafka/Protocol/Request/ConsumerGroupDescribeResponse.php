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
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribedGroup;

/**
 * ConsumerGroupDescribe response object, version 0 (key 69, Kafka 3.7, KIP-848)
 *
 * <pre>
 *   ConsumerGroupDescribe Response (Version: 0) => throttle_time_ms [groups]
 *     throttle_time_ms => INT32
 *     groups           => error_code error_message group_id group_state group_epoch assignment_epoch
 *                         assignor_name [members] authorized_operations
 * </pre>
 *
 * **There is no top-level error code**: every group carries one of its own, as in a DescribeGroups answer. What
 * this answer holds and the classic one cannot is the whole state of the new protocol - the three epochs of the
 * group, the name of the server-side assignor it settled on, and per member the subscription as plain topic names
 * together with **both** assignments, the one it owns and the one it is meant to own
 * ({@see ConsumerGroupDescribedGroup}, {@see \Protocol\Kafka\Protocol\Data\ConsumerGroupDescribeMember}).
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupDescribe API (key 69, v0)"
 */
class ConsumerGroupDescribeResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * The api is flexible from its first version: it was born after KIP-482 (Kafka 2.4)
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Described groups, indexed by the group id
     *
     * @var array<string, ConsumerGroupDescribedGroup>
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
            'groups'         => ['groupId' => ConsumerGroupDescribedGroup::class],
        ];
    }
}
