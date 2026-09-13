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
 *   DeleteGroupsResponseGroup => group_id error_code
 *     group_id   => STRING
 *     error_code => INT16
 * </pre>
 *
 * `GROUP_ERROR_CODE` of `DeleteGroupsResponse.java` @ 1.1.1. The api carries no `error_message`: the code is
 * everything the coordinator says about a group, and the two codes KIP-229 added for it - 68 for a group that still
 * has members and 69 for one the coordinator does not know - are what distinguishes "not deleted" from "never
 * existed".
 *
 * @see docs/protocol/2.8.md, section "DeleteGroups API (key 42, v0 to v2)"
 */
class DeleteGroupsResponseGroup implements BinarySchemaInterface
{
    /**
     * Name of the group this result belongs to
     */
    public string $groupId;

    /**
     * Error code of this group, 0 when the coordinator forgot the group and its offsets
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId'   => BinarySchema::TYPE_STRING,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
