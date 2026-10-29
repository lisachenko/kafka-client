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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of one group of a DeleteGroups answer, i.e. one entry of the `group_error_codes` array
 *
 * <pre>
 *   DeleteGroupsResponseGroup => group_id error_code error_message
 *     group_id      => STRING
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING             -- since version 3 (KIP-1331)
 * </pre>
 *
 * `GROUP_ERROR_CODE` of `DeleteGroupsResponse.java` @ 1.1.1. Up to version 2 the api carries no `error_message`:
 * the code is everything the coordinator says about a group, and the two codes KIP-229 added for it - 68 for a group
 * that still has members and 69 for one the coordinator does not know - are what distinguishes "not deleted" from
 * "never existed".
 *
 * **Kafka 4.4 appended the `error_message` of KIP-1331** behind the code: "Version 3 adds the per-group
 * ErrorMessage field" of `DeleteGroupsResponse.json` @ 4.4.0, a nullable string that is `"ignorable": true` with
 * the default null. It is there for the **134** `GroupDeletionFailed` of a streams group whose stored topology
 * description the plugin of KIP-1331 could not drop - the group is kept and a retry is safe - but the coordinator
 * fills it for every code it refuses a group with. {@see DeleteGroupsResponseGroupV2} is the entry of the versions
 * below 3.
 *
 * @see docs/protocol/4.4.md, section "DeleteGroups API (key 42, v0 to v3)"
 */
class DeleteGroupsResponseGroup implements BinarySchemaInterface
{
    /**
     * Version of the answer this entry belongs to; the version 3 of Kafka 4.4 is the first one with a message
     */
    public const int VERSION = 3;

    /**
     * The version 3 of Kafka 4.4 is the first one whose entries carry an error message (KIP-1331)
     */
    public const int ERROR_MESSAGE_VERSION = 3;

    /**
     * Name of the group this result belongs to
     */
    public string $groupId;

    /**
     * Error code of this group, 0 when the coordinator forgot the group and its offsets
     */
    public int $errorCode;

    /**
     * Why the coordinator refused the group, null for a deleted group and below version 3
     *
     * @since Version 3 of protocol (Kafka 4.4, KIP-1331)
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'groupId'   => BinarySchema::TYPE_STRING,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
        // The message of KIP-1331 is the last field of the entry, behind its code
        if (static::VERSION >= self::ERROR_MESSAGE_VERSION) {
            $scheme['errorMessage'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $scheme;
    }
}
