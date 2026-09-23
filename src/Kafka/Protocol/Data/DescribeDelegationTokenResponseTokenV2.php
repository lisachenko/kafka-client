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
 * One token of a DescribeDelegationToken answer of the versions 0 to 2, i.e. without the requester of KIP-373
 *
 * Kafka 3.3 put the two strings of the principal that **asked** for the token between its owner and its
 * timestamps ({@see DescribeDelegationTokenResponseToken}); this class is the entry of every version below 3, in
 * which the owner is the only principal of a token besides its renewers.
 *
 * @see docs/protocol/4.3.md, section "DescribeDelegationToken API (key 41, v0 to v3)"
 */
final class DescribeDelegationTokenResponseTokenV2 extends DescribeDelegationTokenResponseToken
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
