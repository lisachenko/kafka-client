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
 * RenewDelegationToken response of version 0 (Kafka 1.1), the frame of version 1 with a lower version field
 *
 * <pre>
 *   RenewDelegationToken Response (Version: 0) => error_code expiry_timestamp throttle_time_ms
 * </pre>
 *
 * `TOKEN_RENEW_RESPONSE_V1 = TOKEN_RENEW_RESPONSE_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see RenewDelegationTokenResponse::getScheme()} follows.
 *
 * The answer of version 1 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 1 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "RenewDelegationToken API (key 39, v0 to v2)"
 */
final class RenewDelegationTokenResponseV0 extends RenewDelegationTokenResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
