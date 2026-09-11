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
 *   ListGroups Request (Version: 0, 1 and 2) =>
 * </pre>
 *
 * `LIST_GROUPS_REQUEST_V1 = LIST_GROUPS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3 - both versions are an empty
 * body - and version 1 (KIP-124, Kafka 0.11) added the `throttle_time_ms` to the ANSWER alone
 * ({@see ListGroupsResponse}), so {@see ListGroupsRequestV0} differs in the version field of the header only.
 * Version 2 (KIP-219, Kafka 2.0) is the empty body once more, one api version higher, which is what
 * {@see ListGroupsRequestV1} sends; the request carries a field for the first time at version 4 (KIP-518, Kafka
 * 2.6), the `states_filter`.
 *
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v3)"
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
    public const int VERSION = 3;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }
}
