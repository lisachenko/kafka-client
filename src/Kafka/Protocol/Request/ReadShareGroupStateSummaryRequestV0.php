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
 * ReadShareGroupStateSummary request of version 0 (Kafka 4.1, KIP-932), byte for byte the request of version 1
 *
 * Version 1 (Kafka 4.2, KIP-1226) changed the answer alone: a request of this version is answered with the partitions
 * of {@see ReadShareGroupStateSummaryResponseV0}, which lack the `DeliveryCompleteCount` of version 1. The class exists
 * because a version is a class on this line even when its schema is identical: a frame recorded at version 0 is
 * replayed through the class of version 0.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0 and v1)"
 */
final class ReadShareGroupStateSummaryRequestV0 extends ReadShareGroupStateSummaryRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
