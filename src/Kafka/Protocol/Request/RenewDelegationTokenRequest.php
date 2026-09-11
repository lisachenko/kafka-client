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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * RenewDelegationToken, version 1: moves the expiry of a token forward (ApiKey 39, Kafka 1.1, KIP-48)
 *
 * <pre>
 *   RenewDelegationToken Request (Version: 0 and 1) => hmac renew_time_period
 *     hmac              => BYTES
 *     renew_time_period => INT64
 * </pre>
 *
 * A token is named by its **hmac**, never by its id: `DelegationTokenManager.getToken(hmac)` @ 1.1.1 looks the
 * token up by the raw bytes, which is what makes the hmac the secret half of a token. The bytes are the ones the
 * CreateDelegationToken answer carried; the `kafka-delegation-tokens.sh` tool takes them base64-encoded and decodes
 * them before it builds the frame.
 *
 * `renew_time_period` is a **period** in milliseconds counted from *now*, not a timestamp. The new expiry is
 * `min(maxTimestamp, now + renew_time_period)`, so a renewal can never move the expiry beyond the maximum lifetime
 * the token was issued with, and a negative period ({@see self::DEFAULT_RENEW_TIME_PERIOD}) asks for the
 * `delegation.token.expiry.time.ms` of the broker (24 hours by default).
 *
 * Only the **owner** of the token and the principals its `renewers` name may renew it; anybody else is answered
 * with the error code 63 (`DelegationTokenOwnerMismatch`), see `DelegationTokenManager.allowedToRenew`.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TOKEN_RENEW_REQUEST_V1 =
 * TOKEN_RENEW_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see RenewDelegationTokenRequestV0} is the same frame with the version field of Kafka 1.1.
 *
 * @see docs/protocol/2.8.md, section "RenewDelegationToken API (key 39, v0 and v1)"
 */
class RenewDelegationTokenRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::RENEW_DELEGATION_TOKEN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Asks for the `delegation.token.expiry.time.ms` of the broker instead of a period of its own
     */
    public const int DEFAULT_RENEW_TIME_PERIOD = -1;

    /**
     * @param string $hmac            Raw bytes of the HMAC of the token to renew
     * @param int    $renewTimePeriod Period the expiry moves to, in milliseconds from now, or
     *        {@see self::DEFAULT_RENEW_TIME_PERIOD} for the default renew time of the broker
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * Raw bytes of the HMAC of the token to renew
         */
        protected readonly string $hmac,
        /**
         * Milliseconds from now that the token should stay valid for
         */
        protected readonly int $renewTimePeriod = self::DEFAULT_RENEW_TIME_PERIOD,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the raw bytes of the HMAC of the token this request renews
     */
    public function getHmac(): string
    {
        return $this->hmac;
    }

    /**
     * Returns the period the expiry is moved to, in milliseconds from now
     */
    public function getRenewTimePeriod(): int
    {
        return $this->renewTimePeriod;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'hmac'            => BinarySchema::TYPE_BYTEARRAY,
            'renewTimePeriod' => BinarySchema::TYPE_INT64,
        ];
    }
}
