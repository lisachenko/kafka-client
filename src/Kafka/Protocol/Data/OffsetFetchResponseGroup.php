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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One group of an OffsetFetch answer of version 8 (Kafka 3.0)
 *
 * <pre>
 *   OffsetFetchResponseGroup => group_id [topics] error_code
 *     group_id   => COMPACT_STRING
 *     topics     => OffsetFetchResponseTopic
 *     error_code => INT16
 * </pre>
 *
 * **Version 8 moved the topics and the group-level error code of the answer into an array of these entries**, one
 * per group of the request, and left the answer without a top-level error code altogether:
 * `OffsetFetchResponse.json` @ 3.0.2 keeps `Topics` and `ErrorCode` at the versions `0-7` and gives `Groups` at
 * `8+` a `groupId`, its own `Topics` and its own `ErrorCode`. The layout of a topic and of a partition inside it
 * did not change at all, so the entries are the {@see OffsetFetchResponseTopic} and
 * {@see OffsetFetchResponsePartition} of version 5, `committed_leader_epoch` included.
 *
 * What the error code reports is what the top-level one reported below version 8: the state of the **group** -
 * 15 (`GroupCoordinatorNotAvailable`), 16 (`NotCoordinatorForGroup`), 14 (`GroupLoadInProgress`) or 30
 * (`GroupAuthorizationFailed`) - and an entry that carries one has an empty topics array. A group the coordinator
 * does not know is **not** an error here either: it is answered with an empty topics array and the code 0.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v9)"
 */
class OffsetFetchResponseGroup implements BinarySchemaInterface
{
    /**
     * The group these offsets belong to
     */
    public string $groupId;

    /**
     * Committed offsets of this group, indexed by the topic name
     *
     * @var array<string, OffsetFetchResponseTopic>
     */
    public array $topics = [];

    /**
     * Error of the group itself, which the coordinator reports instead of any topic at all
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Builds an entry from the parts an answer below version 8 carries at its top level
     *
     * @param array<string, OffsetFetchResponseTopic> $topics Committed offsets, indexed by the topic name
     */
    public static function of(string $groupId, array $topics, int $errorCode): self
    {
        $group            = new self();
        $group->groupId   = $groupId;
        $group->topics    = $topics;
        $group->errorCode = $errorCode;

        return $group;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId'   => BinarySchema::TYPE_STRING,
            'topics'    => ['topic' => OffsetFetchResponseTopic::class],
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
