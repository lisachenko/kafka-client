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
 * ConsumerGroupDescribe response of version 0 (Kafka 3.7, KIP-848): members without a `member_type`
 *
 * Version 1 (Kafka 4.0, KIP-1099) closes every member entry with the int8 `member_type`, see
 * {@see ConsumerGroupDescribeResponse}; this is the answer whose members end with their target assignment.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
final class ConsumerGroupDescribeResponseV0 extends ConsumerGroupDescribeResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
