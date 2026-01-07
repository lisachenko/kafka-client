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
 * ListGroups Request
 *
 * This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, you
 * must send ListGroup to all brokers.
 */
class ListGroupsRequest extends AbstractRequest
{
    /**
     * {@inheritdoc}
     */
    public function __construct($correlationId = 0, $clientId = '')
    {
        parent::__construct(ApiKeys::LIST_GROUPS, $correlationId, $clientId, ApiKeys::VERSION_0);
    }
}
