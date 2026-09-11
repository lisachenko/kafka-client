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
 * ExpireDelegationToken, version 0: shortens the life of a token, or ends it now (ApiKey 40, Kafka 1.1, KIP-48)
 *
 * <pre>
 *   ExpireDelegationToken Request (Version: 0) => hmac expiry_time_period
 *     hmac               => BYTES
 *     expiry_time_period => INT64
 * </pre>
 *
 * The api is the mirror image of {@see RenewDelegationTokenRequest} and the same `allowedToRenew` check guards it:
 * the owner of the token and its renewers may expire it, anybody else gets the error code 63.
 *
 * `expiry_time_period` decides between the two things this api does, see `DelegationTokenManager.expireToken`
 * @ 1.1.1:
 *
 *  * a **negative** period ({@see self::EXPIRE_IMMEDIATELY}) deletes the token from ZooKeeper and from the cache of
 *    every broker at once, and the answer carries the `now` of the broker as the expiry timestamp;
 *  * a **non-negative** period sets the expiry to `min(maxTimestamp, now + expiry_time_period)`, which can move the
 *    expiry in either direction as long as it stays below the maximum lifetime - a period of 0 therefore expires
 *    the token now without deleting it.
 *
 * @see docs/protocol/2.8.md, section "ExpireDelegationToken API (key 40, v0)"
 */
class ExpireDelegationTokenRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::EXPIRE_DELEGATION_TOKEN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Deletes the token instead of moving its expiry, i.e. any negative period
     */
    public const int EXPIRE_IMMEDIATELY = -1;

    /**
     * @param string $hmac             Raw bytes of the HMAC of the token to expire
     * @param int    $expiryTimePeriod Period the expiry moves to, in milliseconds from now, or
     *        {@see self::EXPIRE_IMMEDIATELY} to delete the token at once
     * @param string $clientId         A user specified identifier for the client making the request
     * @param int    $correlationId    A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * Raw bytes of the HMAC of the token to expire
         */
        protected readonly string $hmac,
        /**
         * Milliseconds from now that the token should still live, negative to delete it now
         */
        protected readonly int $expiryTimePeriod = self::EXPIRE_IMMEDIATELY,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the raw bytes of the HMAC of the token this request expires
     */
    public function getHmac(): string
    {
        return $this->hmac;
    }

    /**
     * Returns the period the token should still live, in milliseconds from now
     */
    public function getExpiryTimePeriod(): int
    {
        return $this->expiryTimePeriod;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'hmac'             => BinarySchema::TYPE_BYTEARRAY,
            'expiryTimePeriod' => BinarySchema::TYPE_INT64,
        ];
    }
}
