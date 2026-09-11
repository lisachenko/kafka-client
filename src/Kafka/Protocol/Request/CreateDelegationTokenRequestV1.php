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
 * CreateDelegationToken, version 1: the KIP-219 bump of Kafka 2.0 (ApiKey 38)
 *
 * <pre>
 *   CreateDelegationToken Request (Version: 1) => [renewers] max_life_time
 * </pre>
 *
 * The last version of this api in the plain encoding - version 2 (Kafka 2.4) is the first flexible one, and the one
 * {@see CreateDelegationTokenRequest} sends. The frame of v1 is the frame of v0 with a higher number in the header,
 * which is the client's promise that it honours a `throttle_time_ms` itself.
 *
 * @see docs/protocol/2.8.md, section "CreateDelegationToken API (key 38, v0 to v2)"
 */
final class CreateDelegationTokenRequestV1 extends CreateDelegationTokenRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
