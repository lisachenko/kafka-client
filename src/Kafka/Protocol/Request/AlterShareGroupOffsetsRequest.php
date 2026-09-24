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
use Protocol\Kafka\Protocol\Data\AlterShareGroupOffsetsRequestTopic;

/**
 * Sets the share-partition start offsets of a share group (ApiKey 91, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   AlterShareGroupOffsets Request (Version: 0) => GroupId [Topics] TAG_BUFFER
 *     GroupId => COMPACT_STRING
 *     Topics  => COMPACT_ARRAY of {@see AlterShareGroupOffsetsRequestTopic}
 * </pre>
 *
 * What `kafka-share-groups.sh --reset-offsets --execute` sends, and the Java `Admin.alterShareGroupOffsets()`: the
 * start offset every named partition of the group continues at. The request goes to the **group coordinator** of
 * the group, which writes the initializing state of the partitions into its log and hands the offsets to the share
 * coordinator (`InitializeShareGroupState`, key 83) before it answers.
 *
 * Two things of `GroupMetadataManager.alterShareGroupOffsets()` @ 4.3.1 decide what a caller sees: the group is
 * **created** when it does not exist yet ("Get or create the share group"), and a group that has members is refused
 * as a whole with the **68** `NonEmptyGroup` - the offsets of a share group are reset while nobody consumes. A
 * partition of a topic the node does not have, or one beyond the partition count of a topic it has, is answered
 * the **3** `UnknownTopicOrPartition` in its own entry.
 *
 * **Kafka 4.1 added the api** (`AlterShareGroupOffsetsRequest.json` @ 4.1.0, `"validVersions": "0"`, flexible from
 * its version 0, a `broker` listener api).
 *
 * @see docs/protocol/4.3.md, section "AlterShareGroupOffsets API (key 91, v0)"
 */
class AlterShareGroupOffsetsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ALTER_SHARE_GROUP_OFFSETS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics whose start offsets are set, indexed by the topic name
     *
     * @var array<string, AlterShareGroupOffsetsRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string                          $groupId       Id of the share group
     * @param array<string, array<int, int>>  $startOffsets  New start offsets, as topic => partition => offset
     * @param string                          $clientId      A user specified identifier for the client
     * @param int                             $correlationId A value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * Id of the share group
         */
        protected readonly string $groupId,
        array $startOffsets,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $topics = [];
        foreach ($startOffsets as $topic => $partitions) {
            $topics[(string) $topic] = new AlterShareGroupOffsetsRequestTopic((string) $topic, $partitions);
        }
        $this->topics = $topics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groupId' => BinarySchema::TYPE_STRING,
            'topics'  => ['topicName' => AlterShareGroupOffsetsRequestTopic::class],
        ];
    }

    /**
     * Returns the id of the share group whose offsets this request sets
     */
    public function getGroupId(): string
    {
        return $this->groupId;
    }
}
