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

use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;

/**
 * Describe groups response
 *
 * <pre>
 *   DescribeGroupsResponse => [ErrorCode GroupId State ProtocolType Protocol [Members]]
 * </pre>
 *
 * There is no error code for the request as a whole: every group carries its own, and the order of the array is the
 * order of the group ids in the request.
 *
 * @see docs/protocol/0.9.0.md, section "DescribeGroups API (key 15, v0)"
 */
class DescribeGroupsResponse extends AbstractResponse
{
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

        return $header + [
            'groups' => ['groupId' => DescribeGroupResponseMetadata::class],
        ];
    }
}
