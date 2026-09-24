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
use Protocol\Kafka\Protocol\Data\InitializeShareGroupStateRequestTopic;

/**
 * InitializeShareGroupState, version 0: initializes the state of share partitions (ApiKey 83, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   InitializeShareGroupState Request (Version: 0) => group_id [topics]
 *     group_id => COMPACT_STRING
 *     [topics] => topic_id [partitions]
 *       partition    => INT32
 *       state_epoch  => INT32
 *       start_offset => INT64
 * </pre>
 *
 * One of the five **share-group state apis** of KIP-932 (keys 83 to 87), with which the partition leaders and the
 * group coordinators of a cluster talk to the **share coordinator**, the owner of the `__share_group_state` topic.
 * This one carries the state epoch and the start offset a share partition begins with; the share coordinator writes a `ShareSnapshot` record for it. The api is `"listeners": ["broker"]` and unstable in Kafka 3.9; it is stable from
 * Kafka **4.1** on, which is when a client listener serves it. A broker authorizes it as `CLUSTER_ACTION` on the
 * cluster, the operation of inter-broker traffic.
 *
 * **Wire only** on this line, by the owner's decision 2: a class and vectors, no client method. A client that is
 * not a broker has no business sending it; the frames were captured for the grammar and never touch the state of a
 * share group another component owns.
 *
 * @see docs/protocol/4.3.md, section "InitializeShareGroupState API (key 83, v0)"
 */
class InitializeShareGroupStateRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::INITIALIZE_SHARE_GROUP_STATE;

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
     * @var list<InitializeShareGroupStateRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string                                      $groupId       Id of the share group whose state is initialized
     * @param list<InitializeShareGroupStateRequestTopic> $topics        Topics and partitions of that group, each by its topic id
     * @param string                                      $clientId      A user specified identifier for the client
     * @param int                                         $correlationId A value the broker passes back unmodified
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
            'topics'  => [InitializeShareGroupStateRequestTopic::class],
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
     * @return list<InitializeShareGroupStateRequestTopic>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
