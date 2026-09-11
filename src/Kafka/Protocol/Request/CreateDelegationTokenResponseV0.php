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
 * CreateDelegationToken response of version 0 (Kafka 1.1), the frame of version 1 with a lower version field
 *
 * <pre>
 *   CreateDelegationToken Response (Version: 0) => error_code owner issue_timestamp expiry_timestamp
 *                                                  max_timestamp token_id hmac throttle_time_ms
 * </pre>
 *
 * `TOKEN_CREATE_RESPONSE_V1 = TOKEN_CREATE_RESPONSE_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see CreateDelegationTokenResponse::getScheme()} follows.
 *
 * The answer of version 1 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 1 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "CreateDelegationToken API (key 38, v0 to v2)"
 */
final class CreateDelegationTokenResponseV0 extends CreateDelegationTokenResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
