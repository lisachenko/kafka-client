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

/**
 * ReadShareGroupStateSummary answer of version 0 (Kafka 4.1, KIP-932): the summary of a partition without its count
 *
 * Every result is a {@see \Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponseTopicV0}, whose
 * partitions end with the start offset; version 1 (Kafka 4.2, KIP-1226) appended the `DeliveryCompleteCount` of
 * {@see ReadShareGroupStateSummaryResponse}.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0 and v1)"
 */
final class ReadShareGroupStateSummaryResponseV0 extends ReadShareGroupStateSummaryResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
