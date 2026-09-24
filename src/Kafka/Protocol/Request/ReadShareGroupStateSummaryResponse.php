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

use Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponseTopic;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponseTopicV0;

/**
 * ReadShareGroupStateSummary response object, version 1 (key 87, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ReadShareGroupStateSummary Response (Version: 0 to 1) => [results]
 *     results => topic_id [partitions]
 *       partitions => partition error_code error_message state_epoch leader_epoch start_offset
 *                     delivery_complete_count
 *         delivery_complete_count => INT32   -- since version 1 (Kafka 4.2, KIP-1226), "default": -1
 * </pre>
 *
 * **There is no throttle time and no top-level error**: every partition of the request carries the code of its
 * own key (the group, the topic id and the partition), and a request without a topic, without a partition or
 * without a group id is answered with an empty result array.
 *
 * **Version 1 (Kafka 4.2, KIP-1226) appended `DeliveryCompleteCount` to every partition**: "Version 1 introduces
 * DeliveryCompleteCount (KIP-1226)" stands above the `validVersions` of `ReadShareGroupStateSummaryResponse.json`
 * @ 4.2.0. It is the count the partition leader wrote with the last state of the partition (WriteShareGroupState v1),
 * which a group coordinator subtracts, together with the start offset, from the end offset of the partition to
 * report the lag of a share group; -1 is the count of a partition no consumer has read. The results of this class
 * are {@see \Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponseTopic}s, those of
 * {@see ReadShareGroupStateSummaryResponseV0} {@see \Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponseTopicV0}s.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0 and v1)"
 */
class ReadShareGroupStateSummaryResponse extends AbstractResponse
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
     * @var list<ReadShareGroupStateSummaryResponseTopic>
     */
    public array $results = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'results' => [
                static::VERSION >= 1
                    ? ReadShareGroupStateSummaryResponseTopic::class
                    : ReadShareGroupStateSummaryResponseTopicV0::class,
            ],
        ];
    }
}
