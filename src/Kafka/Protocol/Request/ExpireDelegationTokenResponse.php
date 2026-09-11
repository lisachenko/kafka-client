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
 * ExpireDelegationToken response object, version 1 (key 40)
 *
 * <pre>
 *   ExpireDelegationToken Response (Version: 0 and 1) => error_code expiry_timestamp throttle_time_ms
 *     error_code       => INT16
 *     expiry_timestamp => INT64
 *     throttle_time_ms => INT32
 * </pre>
 *
 * The frame is the one of {@see RenewDelegationTokenResponse}, down to the trailing `throttle_time_ms`, and the two
 * schemas of `ExpireDelegationTokenResponse` and `RenewDelegationTokenResponse` @ 1.1.1 are identical field for
 * field - only the meaning of the timestamp differs: a token that was deleted right away is answered with the
 * `now` of the broker, a token whose expiry only moved with that new expiry.
 *
 * The error codes are the ones of the renew api: 61, 62, 63, 64 and 66, see {@see RenewDelegationTokenResponse}.
 * Expiring a token that has already been deleted is the **62** (`DelegationTokenNotFound`), not the 66 - the token
 * is not "expired" for the broker, it is simply gone from ZooKeeper and from the token cache.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TOKEN_EXPIRE_RESPONSE_V1 =
 * TOKEN_EXPIRE_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see ExpireDelegationTokenResponseV0} is the same frame with the version field of Kafka 1.1.
 *
 * **Kafka 2.5 added the version 2** (KIP-482), the same fields in the flexible encoding: every string and array of
 * the frame is compact, the header carries a tag buffer and every structure ends in one. Not a field changed.
 *
 * @see docs/protocol/2.8.md, section "ExpireDelegationToken API (key 40, v0 to v2)"
 */
class ExpireDelegationTokenResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Error code of the request, 0 when the token was expired or deleted
     */
    public int $errorCode = 0;

    /**
     * Milliseconds since the epoch at which the token expires, or expired; -1 for every error
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
