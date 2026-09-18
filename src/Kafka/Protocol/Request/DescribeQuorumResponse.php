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
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopic;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopicV0;

/**
 * DescribeQuorum response object, version 1 (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   DescribeQuorum Response (Version: 0 to 1) => error_code [topics]
 *     error_code => INT16
 *     topics     => topic_name [partitions]
 *       partitions => partition_index error_code leader_id leader_epoch high_watermark
 *                     [current_voters] [observers]
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
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 and v1)"
 */
class DescribeQuorumResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Error of the request as a whole, 0 when every partition carries a result of its own
     */
    public int $errorCode;

    /**
     * Quorum of every topic of the request, indexed by the topic name
     *
     * @var array<string, DescribeQuorumResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode' => BinarySchema::TYPE_INT16,
            'topics'    => ['topicName' => static::topicClass()],
        ];
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class reads
     *
     * @return class-string<DescribeQuorumResponseTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 1 ? DescribeQuorumResponseTopic::class : DescribeQuorumResponseTopicV0::class;
    }
}
