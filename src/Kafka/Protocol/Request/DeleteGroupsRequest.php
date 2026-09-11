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
 * DeleteGroups, version 0: makes the coordinator forget consumer groups (ApiKey 42, Kafka 1.1, KIP-229)
 *
 * <pre>
 *   DeleteGroups Request (Version: 0) => [groups]
 *     groups => STRING
 * </pre>
 *
 * The frame is the one of {@see DescribeGroupsRequest}, and so is the routing: a group is deleted by its own
 * coordinator ({@see GroupCoordinatorRequest}), and every other broker answers it with the error code 16
 * (NotCoordinatorForGroup). An empty group array is legal and is answered with an empty result array.
 *
 * KIP-229 added the api for what `kafka-consumer-groups.sh --delete` had to do through ZooKeeper before:
 * `GroupCoordinator.handleDeleteGroups` @ 1.1.1 removes the group from its cache and writes a tombstone for every
 * committed offset of it into `__consumer_offsets`, so the group disappears from ListGroups and an OffsetFetch of
 * it answers -1 for every partition afterwards. A group is only deletable when it has **no member left**: an
 * `Empty` or a `Dead` group is deleted, anything else is answered with 68 (NonEmptyGroup) - the committed offsets
 * of a group whose consumers are still running are never thrown away by accident.
 *
 * The Java admin client calls the call `deleteConsumerGroups()`, which is the name
 * {@see \Protocol\Kafka\Admin\AdminClient::deleteConsumerGroups()} carries.
 *
 * @see docs/protocol/2.8.md, section "DeleteGroups API (key 42, v0)"
 */
class DeleteGroupsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DELETE_GROUPS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param list<string> $groups        Groups to delete, an empty list is answered with an empty result
     * @param string       $clientId      A user specified identifier for the client making the request
     * @param int          $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        protected array $groups,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->groups = array_values($groups);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groups' => [BinarySchema::TYPE_STRING],
        ];
    }

    /**
     * Returns the groups this request asks to delete
     *
     * @return list<string>
     */
    public function getGroups(): array
    {
        return $this->groups;
    }
}
