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
 * DescribeDelegationToken request of version 2, the first flexible one of the api (Kafka 2.5, KIP-482)
 *
 * Kafka 3.3 added the version 3 and changed no field of the request - "Version 3 adds token requester into the
 * response" of `DescribeDelegationTokenRequest.json` @ 3.3.2 - so this class only lowers the version constant:
 * the version 2 is what a broker below Kafka 3.3 is asked with, and its answer names no requester
 * ({@see DescribeDelegationTokenResponseV2}).
 *
 * @see docs/protocol/3.9.md, section "DescribeDelegationToken API (key 41, v0 to v3)"
 */
final class DescribeDelegationTokenRequestV2 extends DescribeDelegationTokenRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
