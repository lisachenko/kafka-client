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

use Protocol\Kafka\Protocol\Data\ReadShareGroupStateResponseTopic;

/**
 * ReadShareGroupState response object, version 0 (key 84, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ReadShareGroupState Response (Version: 0) => [results]
 *     results => topic_id [partitions]
 * </pre>
 *
 * **There is no throttle time and no top-level error**: every partition of the request carries the code of its
 * own key (the group, the topic id and the partition), and a request without a topic, without a partition or
 * without a group id is answered with an empty result array.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupState API (key 84, v0)"
 */
class ReadShareGroupStateResponse extends AbstractResponse
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
     * Result of every topic of the request, in the order of the answer; empty when the request named none
     *
     * @var list<ReadShareGroupStateResponseTopic>
     */
    public array $results = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'results' => [ReadShareGroupStateResponseTopic::class],
        ];
    }
}
