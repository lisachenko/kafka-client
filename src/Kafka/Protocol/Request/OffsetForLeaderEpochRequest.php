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
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestTopicV0;

/**
 * OffsetForLeaderEpoch, version 2: asks a leader where an epoch of a partition ended (key 23, Kafka 2.1)
 *
 * This is the api of **KIP-101**, "Alter Replication Protocol to use Leader Epoch rather than High Watermark for
 * Truncation". A follower that comes back after a leader change used to truncate its log to the high watermark it
 * remembered, which can diverge from the new leader in two known ways; with this api it asks the new leader
 * instead: "you were the leader of epoch `e` - which offset does that epoch end at?" and truncates to the answer.
 *
 * <pre>
 *   OffsetForLeaderEpoch Request (Version: 2) => [topics]
 *     topics => topic [partitions]
 *       topic      => STRING
 *       partitions => partition_id current_leader_epoch leader_epoch
 *         partition_id         => INT32
 *         current_leader_epoch => INT32     -- since version 2
 *         leader_epoch         => INT32
 * </pre>
 *
 * **Version 1 (Kafka 2.0, KIP-279) left the request untouched** - `OffsetForLeaderEpochRequest.json` @ 2.8.2 says
 * "Version 1 is the same as version 0" - and only added the `leader_epoch` of every partition to the ANSWER, see
 * {@see OffsetForLeaderEpochResponse}. {@see OffsetForLeaderEpochRequestV0} keeps version 0, whose answer carries
 * no epoch.
 *
 * **Version 2 (Kafka 2.1, KIP-320) is the first one that changed the request**: every partition entry gains a
 * `current_leader_epoch` in front of the epoch to look up, see
 * {@see \Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartition::$currentLeaderEpoch}, and the
 * ANSWER gains a leading `throttle_time_ms` - the api is finally one of those a client quota applies to, because
 * from 2.1 on an ordinary **consumer** sends it to validate its position after a leader change.
 * {@see OffsetForLeaderEpochRequestV1} keeps version 1.
 *
 * **This is a broker-to-broker api and this client sends it nowhere.** The classes exist because the api is part
 * of the protocol of the release and this repository documents every api of it with a wire vector of a real
 * broker; a 2.8.2 broker answers an ordinary client just as it answers a follower, because
 * `KafkaApis.handleOffsetForLeaderEpochRequest` @ 2.8.2 authorizes it with `ClusterAction on Cluster`, which a
 * broker without an `authorizer.class.name` grants to everybody.
 *
 * The epoch a client asks with is the `partition_leader_epoch` that the record batches of the partition carry
 * ({@see \Protocol\Kafka\Common\Record\RecordBatch::$partitionLeaderEpoch}), which is the other half of KIP-101.
 *
 * @see docs/protocol/2.8.md, sections "OffsetForLeaderEpoch API (key 23, v0 to v3)" and
 *      "The leader epoch (KIP-320)"
 */
class OffsetForLeaderEpochRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSET_FOR_LEADER_EPOCH;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * `replica_id` of an ordinary consumer, which reads up to the high watermark (KIP-392)
     */
    public const int CONSUMER_REPLICA_ID = -1;

    /**
     * `replica_id` of a debugging client that wants to be served like a replica, the **default** of version 3
     *
     * `OffsetForLeaderEpochRequest.json` @ 2.8.2: "the default is -2 which conventionally represents a *debug*
     * consumer which is allowed to see offsets beyond the high watermark".
     */
    public const int DEBUG_REPLICA_ID = -2;

    /**
     * Epochs to resolve, indexed by the topic they belong to
     *
     * @var array<string, OffsetForLeaderEpochRequestTopic>
     */
    protected readonly array $topics;

    /**
     * A value of the `$topics` map is either the epoch of each partition or an already built topic DTO.
     *
     * @param array<string, array<int, int|array{int, int}|OffsetForLeaderEpochRequestPartition>|OffsetForLeaderEpochRequestTopic> $topics
     *        Leader epoch to resolve, as topic => partition => epoch. A value may also be the pair
     *        `[leaderEpoch, currentLeaderEpoch]`, which is how a caller states the epoch that version 2 (KIP-320)
     *        fences the request on; a plain integer means
     *        {@see \Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartition::UNKNOWN_LEADER_EPOCH}
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $topics,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Node id of the replica that sends this request, since version 3 (Kafka 2.3, KIP-392).
         *
         * A follower names its own broker id here, a consumer {@see self::CONSUMER_REPLICA_ID} (`-1`), and
         * {@see self::DEBUG_REPLICA_ID} (`-2`) is the default of the field: a client that wants to be served like
         * a replica, i.e. to see offsets beyond the high watermark. The field arrived with the follower fetching
         * of KIP-392, because a consumer that reads from a follower validates its position against that follower
         * and the broker has to know which of the two kinds is asking. Every version below 3 has no field at all
         * and is served as a follower would be.
         */
        protected readonly int $replicaId = self::DEBUG_REPLICA_ID
    ) {
        $topicClass   = static::topicClass();
        $packedTopics = [];
        foreach ($topics as $topic => $partitionEpochs) {
            $packedTopics[$topic] = $partitionEpochs instanceof OffsetForLeaderEpochRequestTopic
                ? $partitionEpochs
                : new $topicClass((string) $topic, $partitionEpochs);
        }
        $this->topics = $packedTopics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 3) {
            $body['replicaId'] = BinarySchema::TYPE_INT32;
        }
        $body['topics'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class packs
     *
     * @return class-string<OffsetForLeaderEpochRequestTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 2
            ? OffsetForLeaderEpochRequestTopic::class
            : OffsetForLeaderEpochRequestTopicV0::class;
    }
}
