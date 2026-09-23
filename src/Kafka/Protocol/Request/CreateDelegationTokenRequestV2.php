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
 * CreateDelegationToken request of version 2, the first flexible one of the api (Kafka 2.4, KIP-482)
 *
 * The version 3 of Kafka 3.3 put the two nullable strings of the **owner** of KIP-373 in front of the renewers
 * ({@see CreateDelegationTokenRequest}); this class is the frame without them, which is what a broker below Kafka
 * 3.3 is asked with and what always issues the token for the principal of the connection.
 * {@see CreateDelegationTokenRequestV1} is the same body in the encoding before KIP-482.
 *
 * @see docs/protocol/3.9.md, section "CreateDelegationToken API (key 38, v0 to v3)"
 */
final class CreateDelegationTokenRequestV2 extends CreateDelegationTokenRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
