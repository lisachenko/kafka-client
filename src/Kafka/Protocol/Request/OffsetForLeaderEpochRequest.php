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
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestTopic;

/**
 * OffsetForLeaderEpoch, version 0: asks a leader where an epoch of a partition ended (key 23, Kafka 0.11)
 *
 * This is the api of **KIP-101**, "Alter Replication Protocol to use Leader Epoch rather than High Watermark for
 * Truncation". A follower that comes back after a leader change used to truncate its log to the high watermark it
 * remembered, which can diverge from the new leader in two known ways; with this api it asks the new leader
 * instead: "you were the leader of epoch `e` - which offset does that epoch end at?" and truncates to the answer.
 *
 * <pre>
 *   OffsetForLeaderEpoch Request (Version: 0) => [topics]
 *     topics => topic [partitions]
 *       topic      => STRING
 *       partitions => partition_id leader_epoch
 *         partition_id => INT32
 *         leader_epoch => INT32
 * </pre>
 *
 * **This is a broker-to-broker api and this client sends it nowhere.** The classes exist because the api is part
 * of the protocol of 0.11.0.3 and this repository documents every api of the release with a wire vector of a real
 * broker; a 0.11.0.3 broker answers an ordinary client just as it answers a follower, because
 * `KafkaApis.handleOffsetForLeaderEpochRequest` @ 0.11.0.3 authorizes it with `ClusterAction on Cluster`, which a
 * broker without an `authorizer.class.name` grants to everybody.
 *
 * The epoch a client asks with is the `partition_leader_epoch` that the record batches of the partition carry
 * ({@see \Protocol\Kafka\Common\Record\RecordBatch::$partitionLeaderEpoch}), which is the other half of KIP-101.
 *
 * @see docs/protocol/1.1.md, section "OffsetForLeaderEpoch API (key 23, v0)"
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
    public const int VERSION = 0;

    /**
     * Epochs to resolve, indexed by the topic they belong to
     *
     * @var array<string, OffsetForLeaderEpochRequestTopic>
     */
    protected readonly array $topics;

    /**
     * A value of the `$topics` map is either the epoch of each partition or an already built topic DTO.
     *
     * @param array<string, array<int, int|OffsetForLeaderEpochRequestPartition>|OffsetForLeaderEpochRequestTopic> $topics
     *        Leader epoch to resolve, as topic => partition => epoch
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $topics,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopics = [];
        foreach ($topics as $topic => $partitionEpochs) {
            $packedTopics[$topic] = $partitionEpochs instanceof OffsetForLeaderEpochRequestTopic
                ? $partitionEpochs
                : new OffsetForLeaderEpochRequestTopic((string) $topic, $partitionEpochs);
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

        return $header + [
            'topics' => ['topic' => OffsetForLeaderEpochRequestTopic::class],
        ];
    }
}
