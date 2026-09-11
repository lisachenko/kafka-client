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

use Protocol\Kafka\Protocol\BinarySchema;

/**
 * RenewDelegationToken response object, version 1 (key 39)
 *
 * <pre>
 *   RenewDelegationToken Response (Version: 0 and 1) => error_code expiry_timestamp throttle_time_ms
 *     error_code       => INT16
 *     expiry_timestamp => INT64
 *     throttle_time_ms => INT32
 * </pre>
 *
 * `throttle_time_ms` is the **last** field here as well, see {@see CreateDelegationTokenResponse}.
 *
 * `expiryTimestamp` is the new absolute expiry of the token in milliseconds since the epoch, i.e.
 * `min(maxTimestamp, now + renew_time_period)`; every error carries
 * {@see CreateDelegationTokenResponse::ERROR_TIMESTAMP} (-1) instead, which is
 * `DelegationTokenManager.ErrorTimestamp`.
 *
 * Error codes a 1.1.1 broker answers here:
 *
 * | Code | Name                            | Meaning                                                          |
 * |------|---------------------------------|------------------------------------------------------------------|
 * | 0    | None                            | The expiry moved and is in the answer                            |
 * | 61   | DelegationTokenAuthDisabled     | The broker has no `delegation.token.master.key`                  |
 * | 62   | DelegationTokenNotFound         | No token of the cluster has that hmac (an expired one is gone)   |
 * | 63   | DelegationTokenOwnerMismatch    | The principal is neither the owner nor one of the renewers       |
 * | 64   | DelegationTokenRequestNotAllowed| The connection authenticated nobody, or authenticated with a token |
 * | 66   | DelegationTokenExpired          | The token is past its expiry or its maximum lifetime             |
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TOKEN_RENEW_RESPONSE_V1 =
 * TOKEN_RENEW_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see RenewDelegationTokenResponseV0} is the same frame with the version field of Kafka 1.1.
 *
 * @see docs/protocol/2.8.md, section "RenewDelegationToken API (key 39, v0 and v1)"
 */
class RenewDelegationTokenResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Error code of the request, 0 when the token was renewed
     */
    public int $errorCode = 0;

    /**
     * Milliseconds since the epoch at which the token now expires, -1 for every error
     */
    public int $expiryTimestamp = CreateDelegationTokenResponse::ERROR_TIMESTAMP;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'       => BinarySchema::TYPE_INT16,
            'expiryTimestamp' => BinarySchema::TYPE_INT64,
            'throttleTimeMs'  => BinarySchema::TYPE_INT32,
        ];
    }
}
