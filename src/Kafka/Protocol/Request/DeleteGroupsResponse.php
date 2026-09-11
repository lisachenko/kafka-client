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
use Protocol\Kafka\Protocol\Data\DeleteGroupsResponseGroup;

/**
 * DeleteGroups response object, version 1 (key 42, Kafka 1.1)
 *
 * <pre>
 *   DeleteGroups Response (Version: 0 and 1) => throttle_time_ms [group_error_codes]
 *     throttle_time_ms  => INT32
 *     group_error_codes => group_id error_code
 *       group_id   => STRING
 *       error_code => INT16
 * </pre>
 *
 * There is no top-level error code and no `error_message`: every group of the request gets an entry with a code of
 * its own, in the order the coordinator walked its map. The codes `DeleteGroupsResponse.java` @ 1.1.1 lists are:
 *
 * | Code | Name                        | Meaning                                                                  |
 * |------|-----------------------------|--------------------------------------------------------------------------|
 * | 0    | None                        | The group and its committed offsets are gone                             |
 * | 14   | GroupLoadInProgress         | The coordinator is still reading `__consumer_offsets`                    |
 * | 15   | GroupCoordinatorNotAvailable| The coordinator is shutting down, or the offsets topic is not there yet  |
 * | 16   | NotCoordinatorForGroup      | This broker does not coordinate that group                               |
 * | 24   | InvalidGroupId              | The group id is empty or otherwise invalid                               |
 * | 30   | GroupAuthorizationFailed    | The client may not delete that group                                     |
 * | 68   | NonEmptyGroup               | The group still has members, so nothing was deleted                      |
 * | 69   | GroupIdNotFound             | The coordinator has never heard of that group                            |
 *
 * Version 1 (KIP-219, Kafka 2.0) answers the same bytes, which {@see DeleteGroupsResponseV0} decodes as well.
 *
 * @see docs/protocol/2.8.md, section "DeleteGroups API (key 42, v0 to v2)"
 */
class DeleteGroupsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every group of the request, indexed by the group id
     *
     * @var array<string, DeleteGroupsResponseGroup>
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
            'groups'         => ['groupId' => DeleteGroupsResponseGroup::class],
        ];
    }
}
