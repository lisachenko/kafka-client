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
 * RenewDelegationToken request of version 0 (Kafka 1.1), the frame of version 1 with a lower version field
 *
 * <pre>
 *   RenewDelegationToken Request (Version: 0) => hmac renew_time_period
 * </pre>
 *
 * `TOKEN_RENEW_REQUEST_V1 = TOKEN_RENEW_REQUEST_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see RenewDelegationTokenRequest::getScheme()} follows.
 *
 * What the higher version buys is the promise of KIP-219: a client that sends it honours `throttle_time_ms`
 * itself, so a 2.8.2 broker answers a throttled request of it FIRST and mutes the channel afterwards
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2: "Regardless of throttling, send the response
 * immediately") instead of holding the answer back for the throttle time.
 *
 * @see docs/protocol/2.8.md, section "RenewDelegationToken API (key 39, v0 to v2)"
 */
final class RenewDelegationTokenRequestV0 extends RenewDelegationTokenRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
