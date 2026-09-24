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
 * One group of an OffsetFetch request of version 9 (Kafka 3.7, KIP-848): the entry that names its topics
 *
 * <pre>
 *   OffsetFetchRequestGroup => group_id member_id member_epoch [topics]
 *     topics => name [partition_indexes]    -- NULLABLE
 * </pre>
 *
 * The member id and the member epoch of KIP-848 in front of the topic array of version 8, whose entries name every
 * topic - `OffsetFetchRequest.json` @ 4.2.0 gives the `Name` of a topic entry the versions `8-9`. Version 10 (Kafka
 * 4.2, KIP-848) names the topics by id instead, in the {@see OffsetFetchRequestGroup} of that version.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The member id and epoch of KIP-848 (v9)"
 */
final class OffsetFetchRequestGroupV9 extends OffsetFetchRequestGroup
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
