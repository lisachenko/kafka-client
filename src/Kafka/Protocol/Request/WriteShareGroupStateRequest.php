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
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopicV0;

/**
 * WriteShareGroupState, version 1: writes the state of share partitions (ApiKey 85, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   WriteShareGroupState Request (Version: 0 to 1) => group_id [topics]
 *     group_id => COMPACT_STRING
 *     [topics] => topic_id [partitions]
 *       partition               => INT32
 *       state_epoch             => INT32
 *       leader_epoch            => INT32
 *       start_offset            => INT64
 *       delivery_complete_count => INT32   -- since version 1 (Kafka 4.2, KIP-1226), "default": -1
 *       state_batches => first_offset last_offset delivery_state delivery_count
 * </pre>
 *
 * One of the five **share-group state apis** of KIP-932 (keys 83 to 87), with which the partition leaders and the
 * group coordinators of a cluster talk to the **share coordinator**, the owner of the `__share_group_state` topic.
 * This one carries the delivery state of the records a partition leader has handed out and seen acknowledged, which the share coordinator appends to `__share_group_state`. The api is `"listeners": ["broker"]` and unstable in Kafka 3.9; it is stable from
 * Kafka **4.1** on, which is when a client listener serves it. A broker authorizes it as `CLUSTER_ACTION` on the
 * cluster, the operation of inter-broker traffic.
 *
 * **Wire only** on this line, by the owner's decision 2: a class and vectors, no client method. A client that is
 * not a broker has no business sending it; the frames were captured for the grammar and never touch the state of a
 * share group another component owns.
 *
 * **Version 1 (Kafka 4.2, KIP-1226) added the `DeliveryCompleteCount` of a partition**: "Version 1 introduces
 * DeliveryCompleteCount (KIP-1226)" stands above the `validVersions` of `WriteShareGroupStateRequest.json` @ 4.2.0.
 * The count of the offsets at or above the start offset whose delivery is complete travels with every state a
 * partition leader writes, between the start offset and the batches (see
 * {@see \Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestPartition}). The topics of this class are
 * {@see WriteShareGroupStateRequestTopic}s and those of {@see WriteShareGroupStateRequestV0}, the frame below it,
 * {@see \Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopicV0}s: the constructor converts what it is
 * given into the entries of its own version.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
class WriteShareGroupStateRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::WRITE_SHARE_GROUP_STATE;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics of this request, in the order they were given
     *
     * @var list<WriteShareGroupStateRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string                                 $groupId       Id of the share group whose state is written
     * @param list<WriteShareGroupStateRequestTopic> $topics        Topics and partitions of that group, each by its topic id
     * @param string                                 $clientId      A user specified identifier for the client
     * @param int                                    $correlationId A value the broker passes back unmodified
     */
    public function __construct(
        protected readonly string $groupId,
        array $topics,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $topicClass = static::topicClass();
        $converted  = [];
        foreach ($topics as $topic) {
            $converted[] = $topic::class === $topicClass
                ? $topic
                : new $topicClass($topic->topicId, array_values($topic->partitions));
        }
        $this->topics = $converted;

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
            'topics'  => [static::topicClass()],
        ];
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class sends
     *
     * @return class-string<WriteShareGroupStateRequestTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 1 ? WriteShareGroupStateRequestTopic::class : WriteShareGroupStateRequestTopicV0::class;
    }

    /**
     * Returns the id of the share group this request is about
     */
    public function getGroupId(): string
    {
        return $this->groupId;
    }

    /**
     * Returns the topics of this request, in the order they were given
     *
     * @return list<WriteShareGroupStateRequestTopic>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
