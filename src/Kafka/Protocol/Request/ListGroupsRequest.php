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
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * This API lists the groups that the broker it is sent to is the coordinator of.
 *
 * The request has no body at all, only the common request header. Every broker answers with its own groups, so a
 * client that wants the groups of the whole cluster has to ask each of them - see
 * {@see \Protocol\Kafka\Admin\AdminClient::listAllGroups()}.
 *
 * <pre>
 *   ListGroups Request (Version: 0 to 3) =>
 *
 *   ListGroups Request (Version: 4) => [states_filter]
 *     states_filter => STRING     -- since version 4
 * </pre>
 *
 * `LIST_GROUPS_REQUEST_V1 = LIST_GROUPS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3 - both versions are an empty
 * body - and version 1 (KIP-124, Kafka 0.11) added the `throttle_time_ms` to the ANSWER alone
 * ({@see ListGroupsResponse}), so {@see ListGroupsRequestV0} differs in the version field of the header only.
 * Version 2 (KIP-219, Kafka 2.0) is the empty body once more, one api version higher, which is what
 * {@see ListGroupsRequestV1} sends, and version 3 (KIP-482, Kafka 2.4) is the same nothing in the flexible
 * encoding - a body of one empty tag buffer - which {@see ListGroupsRequestV3} sends.
 *
 * **Version 4 (KIP-518, Kafka 2.6) gave the request its first field**, the `states_filter`: an array of group
 * state names that bounds the answer to the groups in one of them. An **empty** array is "every group", and so is
 * a null one - `KafkaApis.handleListGroupsRequest` @ 2.8.2 turns a null filter into the empty set with the comment
 * "Handle a null array the same as empty", and `GroupCoordinator.handleListGroups` answers every group it
 * coordinates when the set is empty. The names are matched against `GroupMetadata.state.toString` verbatim, which
 * makes the filter case sensitive; the constants of {@see \Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata}
 * are those names.
 *
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v4)"
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
    public const int VERSION = 4;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * @param string       $clientId      A user specified identifier for the client making the request
     * @param int          $correlationId A user-supplied value that the broker passes back unmodified
     * @param list<string> $statesFilter  States of the groups to list (KIP-518, version 4); an empty list asks
     *        for every group the broker coordinates, which is what every version below 4 asks for implicitly
     */
    public function __construct(
        string $clientId = '',
        int $correlationId = 0,
        /**
         * States of the groups this request asks for, empty for every group.
         *
         * @var list<string>
         *
         * @since Version 4 of protocol
         */
        protected readonly array $statesFilter = []
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 4) {
            $body['statesFilter'] = [BinarySchema::TYPE_STRING];
        }

        return $header + $body;
    }

    /**
     * Returns the states this request bounds the answer to, empty for every group
     *
     * @return list<string>
     */
    public function getStatesFilter(): array
    {
        return $this->statesFilter;
    }
}
