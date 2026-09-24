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

use Protocol\Kafka\Protocol\Data\ShareGroupStateTopicResult;

/**
 * WriteShareGroupState response object, version 1 (key 85, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   WriteShareGroupState Response (Version: 0 to 1) => [results]
 *     results => topic_id [partitions]
 * </pre>
 *
 * **There is no throttle time and no top-level error**: every partition of the request carries the code of its
 * own key (the group, the topic id and the partition), and a request without a topic, without a partition or
 * without a group id is answered with an empty result array.
 *
 * **Version 1 (Kafka 4.2, KIP-1226) is the frame of version 0**: "Version 1 introduces DeliveryCompleteCount in the
 * request (KIP-1226)" is the comment above the `validVersions` of `WriteShareGroupStateResponse.json` @ 4.2.0, and
 * the answer declares no new field. {@see WriteShareGroupStateResponseV0} is the answer of a
 * {@see WriteShareGroupStateRequestV0}.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
class WriteShareGroupStateResponse extends AbstractResponse
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
     * Result of every topic of the request, in the order of the answer; empty when the request named none
     *
     * @var list<ShareGroupStateTopicResult>
     */
    public array $results = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'results' => [ShareGroupStateTopicResult::class],
        ];
    }
}
