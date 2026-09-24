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

namespace Protocol\Kafka\Protocol\Data;

/**
 * The results of one topic of a ReadShareGroupStateSummary answer of version 0 (Kafka 4.1, KIP-932)
 *
 * The same topic id and partition array as {@see ReadShareGroupStateSummaryResponseTopic}; the entries are
 * {@see ReadShareGroupStateSummaryResponsePartitionV0}, without the `DeliveryCompleteCount` of version 1 (Kafka 4.2,
 * KIP-1226).
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0 and v1)"
 */
final class ReadShareGroupStateSummaryResponseTopicV0 extends ReadShareGroupStateSummaryResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
