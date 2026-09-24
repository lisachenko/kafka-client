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
 * WriteShareGroupState request of version 0 (Kafka 4.1, KIP-932): the state of a partition without its count
 *
 * The frame of {@see WriteShareGroupStateRequest} whose partitions lack the `DeliveryCompleteCount` that version 1
 * (Kafka 4.2, KIP-1226) put between the start offset and the batches. Its topics are
 * {@see \Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopicV0}s, into which the constructor converts the
 * topics it is given, so the count of a partition never reaches the wire.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
final class WriteShareGroupStateRequestV0 extends WriteShareGroupStateRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
