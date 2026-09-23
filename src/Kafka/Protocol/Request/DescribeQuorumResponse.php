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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseNode;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopic;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopicV0;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopicV1;

/**
 * DescribeQuorum response object, version 2 (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   DescribeQuorum Response (Version: 0 to 2) => error_code error_message [topics] [nodes]
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING             -- since version 2
 *     topics        => topic_name [partitions]
 *       partitions  => partition_index error_code error_message leader_id leader_epoch high_watermark
 *                      [current_voters] [observers]
 *     nodes         => node_id [listeners]         -- since version 2
 *       listeners   => name host port
 * </pre>
 *
 * **There is no `throttle_time_ms`**: the api is not part of the quota machinery, which is why its answer opens
 * with the top-level error code instead. That code is the one of the request as a whole - the authorization of
 * the cluster, for instance - while a partition that could not be described carries one of its own.
 *
 * **Version 1 (KIP-836, Kafka 3.3) is the reason this answer has a version at all**: it appended
 * `LastFetchTimestamp` and `LastCaughtUpTimestamp` to every voter and observer entry, see
 * {@see \Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaState}. {@see DescribeQuorumResponseV0} is the
 * same answer with the shorter entries.
 *
 * **Version 2 (KIP-853, Kafka 3.9) added four things**, and it is the version this client sends: an
 * `ErrorMessage` next to the top-level code, an `ErrorMessage` next to the code of every partition, a
 * `ReplicaDirectoryId` inside every replica state, and the top-level **`Nodes`** array - one entry per node of
 * the quorum with the listener name, host and port it can be reached at
 * ({@see \Protocol\Kafka\Protocol\Data\DescribeQuorumResponseNode}). The last one is what the reconfiguration apis
 * of the same KIP need: a replica state names a voter by its id and its directory id, and the endpoint of that id
 * is only here. {@see DescribeQuorumResponseV1} reads the answer without any of the four.
 *
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 to v2)"
 */
class DescribeQuorumResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Error of the request as a whole, 0 when every partition carries a result of its own
     */
    public int $errorCode;

    /**
     * Text of that error, the empty string when there was none and null when the answer left the field out
     *
     * @since Version 2 of protocol (Kafka 3.9, KIP-853)
     */
    public ?string $errorMessage = null;

    /**
     * Quorum of every topic of the request, indexed by the topic name
     *
     * @var array<string, DescribeQuorumResponseTopic>
     */
    public array $topics = [];

    /**
     * Every node of the quorum with its endpoints, indexed by the node id
     *
     * Empty in an answer below the version 2, which has no such array at all.
     *
     * @var array<int, DescribeQuorumResponseNode>
     *
     * @since Version 2 of protocol (Kafka 3.9, KIP-853)
     */
    public array $nodes = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        $body = ['errorCode' => BinarySchema::TYPE_INT16];
        if (static::VERSION >= 2) {
            $body['errorMessage'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $body['topics'] = ['topicName' => static::topicClass()];
        if (static::VERSION >= 2) {
            $body['nodes'] = ['nodeId' => DescribeQuorumResponseNode::class];
        }

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class reads
     *
     * @return class-string<DescribeQuorumResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 2 => DescribeQuorumResponseTopic::class,
            static::VERSION >= 1 => DescribeQuorumResponseTopicV1::class,
            default              => DescribeQuorumResponseTopicV0::class,
        };
    }
}
