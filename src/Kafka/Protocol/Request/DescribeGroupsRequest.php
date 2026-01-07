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
/**
 * @author Alexander.Lisachenko
 * @date 28.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * DescribeGroups Request
 *
 * This API can be used to describe the current groups managed by a broker. To get a list of all groups in the cluster, you
 * must send DescribeGroups to all brokers.
 */
class DescribeGroupsRequest extends AbstractRequest
{
    /**
     * {@inheritdoc}
     */
    public function __construct(/**
     * List of groups to describe
     */
        private readonly array $groups,
        $correlationId = 0,
        $clientId = ''
    ) {
        parent::__construct(ApiKeys::DESCRIBE_GROUPS, $correlationId, $clientId);
    }

    /**
     * @inheritDoc
     * DescribeGroupsRequest => [GroupId]
     *   GroupId => string
     */
    protected function packPayload(): string
    {
        $payload      = parent::packPayload();
        $payload .= pack('N', count($this->groups));

        foreach ($this->groups as $group) {
            $groupLength = strlen($group);
            $payload .= pack("na{$groupLength}", $groupLength, $group);
        }

        return $payload;
    }
}
