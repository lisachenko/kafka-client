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
 * OffsetFetch request of version 8 (Kafka 3.0): the batch of groups before KIP-848 named a member in it
 *
 * Version 9 (Kafka 3.7) added a nullable `member_id` and a `member_epoch` to every group entry of this very
 * frame and changed nothing else, so this version is the batched request without them - the highest one a broker
 * below Kafka 3.7 serves, and the version a member of a KIP-848 group must not use to read its own offsets,
 * because the coordinator has no field to validate it with. {@see OffsetFetchRequest} sends the version 9.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v9)"
 * @see docs/protocol/4.3.md, section "The member id and epoch of KIP-848 (v9)"
 */
final class OffsetFetchRequestV8 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
