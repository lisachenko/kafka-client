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
 * One described group of a ConsumerGroupDescribe answer of version 0 (Kafka 3.7, KIP-848)
 *
 * The group entry itself did not change at version 1 (Kafka 4.0, KIP-1099); its **members** did, and this class
 * decodes them as {@see ConsumerGroupDescribeMemberV0}, the entries without a `member_type`.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
final class ConsumerGroupDescribedGroupV0 extends ConsumerGroupDescribedGroup
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
