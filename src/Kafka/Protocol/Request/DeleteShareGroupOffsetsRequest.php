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
use Protocol\Kafka\Protocol\Data\DeleteShareGroupOffsetsRequestTopic;

/**
 * Deletes the share-group state of topics from a share group (ApiKey 92, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DeleteShareGroupOffsets Request (Version: 0) => GroupId [Topics] TAG_BUFFER
 *     GroupId => COMPACT_STRING
 *     Topics  => COMPACT_ARRAY of {@see DeleteShareGroupOffsetsRequestTopic}
 * </pre>
 *
 * What `kafka-share-groups.sh --delete-offsets --topic …` sends, and the Java `Admin.deleteShareGroupOffsets()`:
 * the group forgets where it stands in **whole topics** - there is no partition list - and a later member of the
 * group starts them over at `share.auto.offset.reset`. The request goes to the **group coordinator** of the group,
 * which has the share coordinator delete the state (`DeleteShareGroupState`, key 86). Unlike an alter, a delete
 * does not create the group: a group the coordinator does not hold is the top-level **69** `GroupIdNotFound`, and
 * one that has members the **68** `NonEmptyGroup`.
 *
 * **Kafka 4.1 added the api** (`DeleteShareGroupOffsetsRequest.json` @ 4.1.0, `"validVersions": "0"`, flexible
 * from its version 0, a `broker` listener api).
 *
 * @see docs/protocol/4.3.md, section "DeleteShareGroupOffsets API (key 92, v0)"
 */
class DeleteShareGroupOffsetsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DELETE_SHARE_GROUP_OFFSETS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics whose state is deleted, indexed by the topic name
     *
     * @var array<string, DeleteShareGroupOffsetsRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string       $groupId       Id of the share group
     * @param list<string> $topics        Topics whose share-group state is deleted
     * @param string       $clientId      A user specified identifier for the client making the request
     * @param int          $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * Id of the share group
         */
        protected readonly string $groupId,
        array $topics,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $entries = [];
        foreach ($topics as $topic) {
            $entries[$topic] = new DeleteShareGroupOffsetsRequestTopic($topic);
        }
        $this->topics = $entries;

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
            'topics'  => ['topicName' => DeleteShareGroupOffsetsRequestTopic::class],
        ];
    }

    /**
     * Returns the id of the share group this request deletes offsets of
     */
    public function getGroupId(): string
    {
        return $this->groupId;
    }
}
