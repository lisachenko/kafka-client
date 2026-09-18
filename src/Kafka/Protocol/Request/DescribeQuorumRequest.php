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
use Protocol\Kafka\Protocol\Data\DescribeQuorumRequestTopic;

/**
 * DescribeQuorum, version 1: the state of a raft quorum (ApiKey 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   DescribeQuorum Request (Version: 0 to 1) => [topics]
 *     topics => topic_name [partitions]
 *       topic_name => COMPACT_STRING
 *       partitions => partition_index
 *         partition_index => INT32
 * </pre>
 *
 * KIP-595 replaced ZooKeeper with a **raft quorum** of Kafka's own, and this is the api that reads its state: who
 * leads it, in which epoch, how far it is replicated, and how far behind every voter and observer of it is. The
 * quorum replicates one partition - the partition 0 of {@see self::CLUSTER_METADATA_TOPIC} - so a request that
 * asks about anything else is a request about a topic that has no quorum at all.
 *
 * **It is the one raft api of KIP-595 that a client can send.** `DescribeQuorumRequest.json` @ 3.3.2 declares
 * `"listeners": ["broker", "controller"]`, while `Vote` (52), `BeginQuorumEpoch` (53) and `EndQuorumEpoch` (54)
 * are `controller` apis and never answered on a client listener.
 *
 * **Version 1 (KIP-836, Kafka 3.3) did not change this half**: "Version 1 adds additional fields in the response.
 * The request is unchanged (KIP-836)" is the comment above its `validVersions` in the specification, so
 * {@see DescribeQuorumRequestV0} sends these very bytes one api version lower and is answered with
 * {@see DescribeQuorumResponseV0}, whose replica states have no timestamps.
 *
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 and v1)"
 */
class DescribeQuorumRequest extends AbstractRequest
{
    /**
     * The one topic a KRaft cluster replicates with its raft quorum (`Topic.CLUSTER_METADATA_TOPIC_NAME` @ 3.3.2)
     */
    public const string CLUSTER_METADATA_TOPIC = '__cluster_metadata';

    /**
     * The one partition of that topic (`Topic.CLUSTER_METADATA_TOPIC_PARTITION` @ 3.3.2)
     */
    public const int CLUSTER_METADATA_PARTITION = 0;

    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_QUORUM;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics of this request, indexed by their name
     *
     * @var array<string, DescribeQuorumRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param array<string, list<int>> $topicPartitions Partitions to describe the quorum of, per topic;
     *        {@see self::metadataQuorum()} builds the one request a KRaft cluster answers
     * @param string                   $clientId        A user specified identifier for the client
     * @param int                      $correlationId   A value the broker passes back unmodified
     */
    public function __construct(array $topicPartitions, string $clientId = '', int $correlationId = 0)
    {
        $topics = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $topics[(string) $topic] = new DescribeQuorumRequestTopic((string) $topic, array_values($partitions));
        }
        $this->topics = $topics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the request about the metadata quorum of the cluster, which is the only quorum there is
     *
     * `DescribeQuorumRequest.singletonRequest(CLUSTER_METADATA_TOPIC_PARTITION)` @ 3.3.2, the request every
     * `Admin.describeMetadataQuorum()` sends.
     */
    public static function metadataQuorum(string $clientId = '', int $correlationId = 0): static
    {
        return new static(
            [self::CLUSTER_METADATA_TOPIC => [self::CLUSTER_METADATA_PARTITION]],
            $clientId,
            $correlationId
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topicName' => DescribeQuorumRequestTopic::class],
        ];
    }

    /**
     * Returns the topics of this request, indexed by their name
     *
     * @return array<string, DescribeQuorumRequestTopic>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
