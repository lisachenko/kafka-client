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
 * ConsumerGroupDescribe request of version 0 (Kafka 3.7, KIP-848)
 *
 * The request of version 1 (Kafka 4.0, KIP-1099) is byte for byte this one but for the api version of its header,
 * which {@see ConsumerGroupDescribeRequest} sends: the version asks for the answer that gives every member its
 * `member_type`, and this one for the answer without it.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
final class ConsumerGroupDescribeRequestV0 extends ConsumerGroupDescribeRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
