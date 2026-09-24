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
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestTopic;

/**
 * ReadShareGroupStateSummary, version 0: reads the summary of the state of share partitions (ApiKey 87, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ReadShareGroupStateSummary Request (Version: 0) => group_id [topics]
 *     group_id => COMPACT_STRING
 *     [topics] => topic_id [partitions]
 *       partition    => INT32
 *       leader_epoch => INT32
 * </pre>
 *
 * One of the five **share-group state apis** of KIP-932 (keys 83 to 87), with which the partition leaders and the
 * group coordinators of a cluster talk to the **share coordinator**, the owner of the `__share_group_state` topic.
 * This one carries the state epoch, the leader epoch and the start offset of a share partition without its batches, which a group coordinator reads to answer DescribeShareGroupOffsets (90); the request is the frame of ReadShareGroupState and carries its topics as {@see \Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestTopic}. The api is `"listeners": ["broker"]` and unstable in Kafka 3.9; it is stable from
 * Kafka **4.1** on, which is when a client listener serves it. A broker authorizes it as `CLUSTER_ACTION` on the
 * cluster, the operation of inter-broker traffic.
 *
 * **Wire only** on this line, by the owner's decision 2: a class and vectors, no client method. A client that is
 * not a broker has no business sending it; the frames were captured for the grammar and never touch the state of a
 * share group another component owns.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0)"
 */
class ReadShareGroupStateSummaryRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::READ_SHARE_GROUP_STATE_SUMMARY;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics of this request, in the order they were given
     *
     * @var list<ReadShareGroupStateRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string                                $groupId       Id of the share group whose state is summarized
     * @param list<ReadShareGroupStateRequestTopic> $topics        Topics and partitions of that group, each by its topic id
     * @param string                                $clientId      A user specified identifier for the client
     * @param int                                   $correlationId A value the broker passes back unmodified
     */
    public function __construct(
        protected readonly string $groupId,
        array $topics,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->topics = array_values($topics);

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
            'topics'  => [ReadShareGroupStateRequestTopic::class],
        ];
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
     * @return list<ReadShareGroupStateRequestTopic>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
