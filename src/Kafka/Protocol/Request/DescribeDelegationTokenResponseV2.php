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
 * DescribeDelegationToken answer of version 2, the frame before the token requester of KIP-373
 *
 * The version 3 of Kafka 3.3 added the two strings of the **requester** of each token behind its owner
 * ({@see \Protocol\Kafka\Protocol\Data\DescribeDelegationTokenResponseToken}); the tokens of this answer are read
 * through {@see \Protocol\Kafka\Protocol\Data\DescribeDelegationTokenResponseTokenV2}, which has no such field.
 * {@see DescribeDelegationTokenResponseV1} is the same body in the encoding before KIP-482.
 *
 * @see docs/protocol/4.3.md, section "DescribeDelegationToken API (key 41, v0 to v3)"
 */
final class DescribeDelegationTokenResponseV2 extends DescribeDelegationTokenResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
