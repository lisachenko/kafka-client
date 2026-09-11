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
use Protocol\Kafka\Protocol\Data\DescribeProducersResponseTopic;

/**
 * DescribeProducers response object, version 0 (key 61, Kafka 2.8, KIP-664)
 *
 * <pre>
 *   DescribeProducers Response (Version: 0) => throttle_time_ms [topics]
 *     throttle_time_ms => INT32
 *     topics           => name [partitions]
 *       partitions => partition_index error_code error_message [active_producers]
 * </pre>
 *
 * **There is no top-level error code**: every partition of the request carries one of its own, and a partition
 * without producer state is the code 0 with an empty producer array.
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
class DescribeProducersResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every topic of the request, indexed by the topic name
     *
     * @var array<string, DescribeProducersResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'topics'         => ['name' => DescribeProducersResponseTopic::class],
        ];
    }
}
