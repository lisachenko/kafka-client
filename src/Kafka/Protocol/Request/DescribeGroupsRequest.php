<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * DescribeGroups Request
 *
 * This API can be used to describe the current groups managed by a broker. To get a list of all groups in the cluster, you
 * must send DescribeGroups to all brokers.
 *
 * DescribeGroups Request (Version: 0) => [group_ids]
 *   group_ids => STRING
 */
class DescribeGroupsRequest extends AbstractRequest
{
    public function __construct(/**
     * List of groups to describe
     */
        private readonly array $groups,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(ApiKeys::DESCRIBE_GROUPS, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = null;

        return $header + [
            'groups' => [BinarySchema::TYPE_STRING],
        ];
    }
}
