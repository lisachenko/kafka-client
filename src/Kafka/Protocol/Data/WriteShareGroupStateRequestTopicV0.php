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
 * One topic of a WriteShareGroupState request of version 0 (Kafka 4.1, KIP-932)
 *
 * The same topic id and partition array as {@see WriteShareGroupStateRequestTopic}; what differs is the entry of
 * the array, a {@see WriteShareGroupStateRequestPartitionV0} without the `DeliveryCompleteCount` that version 1
 * (Kafka 4.2, KIP-1226) added. A partition of version 1 given to this class is converted into it.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
final class WriteShareGroupStateRequestTopicV0 extends WriteShareGroupStateRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
