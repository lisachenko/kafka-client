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

/**
 * This API lists the groups that the broker it is sent to is the coordinator of.
 *
 * The request has no body at all, only the common request header. Every broker answers with its own groups, so a
 * client that wants the groups of the whole cluster has to ask each of them - see
 * {@see \Protocol\Kafka\Admin\AdminClient::listAllGroups()}.
 *
 * <pre>
 *   ListGroupsRequest =>
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "ListGroups API (key 16, v0)"
 */
class ListGroupsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::LIST_GROUPS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }
}
