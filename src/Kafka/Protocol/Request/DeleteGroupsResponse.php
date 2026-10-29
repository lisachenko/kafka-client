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
use Protocol\Kafka\Protocol\Data\DeleteGroupsResponseGroupV2;

/**
 * DeleteGroups response object, version 3 (key 42, Kafka 1.1)
 *
 * <pre>
 *   DeleteGroups Response (Version: 3) => throttle_time_ms [group_error_codes]
 *     throttle_time_ms  => INT32
 *     group_error_codes => group_id error_code error_message
 *       group_id      => STRING
 *       error_code    => INT16
 *       error_message => NULLABLE_STRING           -- since version 3
 * </pre>
 *
 * There is no top-level error code: every group of the request gets an entry with a code of its own, in the order
 * the coordinator walked its map. The codes `DeleteGroupsResponse.java` @ 1.1.1 lists are:
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
 * | 134  | GroupDeletionFailed         | (Kafka 4.4, v3) The topology description plugin could not drop the group |
 *
 * Version 1 (KIP-219, Kafka 2.0) answers the same bytes, which {@see DeleteGroupsResponseV0} decodes as well.
 *
 * **Kafka 4.4 added the version 3 and with it the `error_message` of KIP-1331** ("Version 3 adds the per-group
 * ErrorMessage field" of `DeleteGroupsResponse.json` @ 4.4.0): a nullable string at the **end of every group entry**,
 * behind its code, for the new code **134** `GroupDeletionFailed` - a streams group whose stored topology
 * description the plugin of KIP-1331 (`group.streams.topology.description.plugin.class`) could not drop; the group is
 * kept and a retry is safe. A request below version 3 never sees the 134: `KafkaApis.handleDeleteGroupsRequest` @
 * 4.4.0 downgrades it to the -1 `UnknownServerError` there. Every other code keeps the null message (measured on
 * the 4.4.0 node). {@see DeleteGroupsResponseV2} decodes the answer of a broker below Kafka 4.4, whose entries end
 * with the code.
 *
 * @see docs/protocol/4.4.md, section "DeleteGroups API (key 42, v0 to v3)"
 */
class DeleteGroupsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * The version 3 of Kafka 4.4 is the first one whose group entries carry an error message (KIP-1331)
     */
    public const int ERROR_MESSAGE_VERSION = 3;

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
            'groups'         => ['groupId' => static::groupClass()],
        ];
    }

    /**
     * Returns the class of a group entry for the version of the API that this class decodes
     *
     * @return class-string<DeleteGroupsResponseGroup>
     */
    protected static function groupClass(): string
    {
        return static::VERSION >= self::ERROR_MESSAGE_VERSION
            ? DeleteGroupsResponseGroup::class
            : DeleteGroupsResponseGroupV2::class;
    }
}
