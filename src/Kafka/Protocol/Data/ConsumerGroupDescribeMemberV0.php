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
 * One member of a group in a ConsumerGroupDescribe answer of version 0 (Kafka 3.7, KIP-848)
 *
 * Version 1 (Kafka 4.0, KIP-1099) appended the int8 `member_type` behind the target assignment, see
 * {@see ConsumerGroupDescribeMember}; this is the entry without it, whose type stays
 * {@see ConsumerGroupDescribeMember::MEMBER_TYPE_UNKNOWN}.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
final class ConsumerGroupDescribeMemberV0 extends ConsumerGroupDescribeMember
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
