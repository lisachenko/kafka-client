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

use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\InlineStruct;

/**
 * CreateDelegationToken response object, version 3 (key 38)
 *
 * <pre>
 *   CreateDelegationToken Response (Version: 0 and 1) => error_code owner issue_timestamp expiry_timestamp max_timestamp
 *                                                  token_id hmac throttle_time_ms
 *     error_code => INT16
 *     owner      => principal_type name
 *       principal_type => STRING
 *       name           => STRING
 *     issue_timestamp  => INT64
 *     expiry_timestamp => INT64
 *     max_timestamp    => INT64
 *     token_id         => STRING
 *     hmac             => BYTES
 *     throttle_time_ms => INT32
 * </pre>
 *
 * **`throttle_time_ms` is the LAST field of this answer, not the first one.** KIP-124 put the field in front of
 * every answer of Kafka 0.11, but the apis that were added afterwards append it instead: `CommonFields.THROTTLE_TIME_MS`
 * is written with `setIfExists` at the end of `toStruct` @ 1.1.1, and all four delegation token apis, DescribeLogDirs
 * (35), AlterReplicaLogDirs (34) and SaslAuthenticate (36) follow that shape. Verified on the wire against the
 * container, see the vector `createdelegationtoken.response.v0`.
 *
 * The three timestamps are absolute milliseconds since the epoch, as `DelegationTokenManager.createToken` @ 1.1.1
 * computes them from the clock of the broker:
 *
 * | Field              | Value                                                                                  |
 * |--------------------|----------------------------------------------------------------------------------------|
 * | `issueTimestamp`   | `time.milliseconds()` of the broker when it issued the token                            |
 * | `maxTimestamp`     | `issueTimestamp + min(max_life_time, delegation.token.max.lifetime.ms)`, the hard end   |
 * | `expiryTimestamp`  | `min(maxTimestamp, issueTimestamp + delegation.token.expiry.time.ms)`, the renewable end |
 *
 * A failed request carries the error code, the principal of the connection as the owner and the "no token" values
 * of `CreateDelegationTokenResponse(throttleTimeMs, error, owner)` @ 1.1.1: the three timestamps are -1, the token
 * id is the empty string and the hmac is an empty byte array.
 *
 * Error codes a 1.1.1 broker answers here:
 *
 * | Code | Name                            | Meaning                                                          |
 * |------|---------------------------------|------------------------------------------------------------------|
 * | 0    | None                            | The token was issued and is in the answer                        |
 * | 61   | DelegationTokenAuthDisabled     | The broker has no `delegation.token.master.key`                  |
 * | 64   | DelegationTokenRequestNotAllowed| The connection authenticated nobody, or authenticated with a token |
 * | 67   | InvalidPrincipalType            | A renewer of the request is not of the type `User`               |
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TOKEN_CREATE_RESPONSE_V1 =
 * TOKEN_CREATE_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see CreateDelegationTokenResponseV0} is the same frame with the version field of Kafka 1.1.
 *
 * **Kafka 3.3 added version 3 and with it the token requester** ("Version 3 adds token requester details" of
 * `CreateDelegationTokenResponse.json` @ 3.3.2): two strings behind the owner - `token_requester_principal_type`
 * and `token_requester_principal_name` - that say **who asked** for the token, which is a question the answer
 * could not have before KIP-373 made the owner a field of the request. For a token a principal issues for
 * itself the two principals are the same; for a token issued for somebody else the owner is the principal of the
 * request and the requester is the caller. {@see CreateDelegationTokenResponseV2} decodes the answer of every
 * version below 3, where {@see self::$tokenRequester} stays the owner.
 *
 * @see docs/protocol/4.3.md, section "CreateDelegationToken API (key 38, v0 to v3)"
 */
class CreateDelegationTokenResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * The version 3 of Kafka 3.3 is the first one whose answer names the requester of the token (KIP-373)
     */
    public const int TOKEN_REQUESTER_VERSION = 3;

    /**
     * Timestamp of an answer that carries no token at all, `DelegationTokenManager.ErrorTimestamp`
     */
    public const int ERROR_TIMESTAMP = -1;

    /**
     * Error code of the request, 0 when the token was issued
     */
    public int $errorCode = 0;

    /**
     * Principal the token was issued for, i.e. the principal of the connection
     */
    public KafkaPrincipal $owner;

    /**
     * Principal that asked for the token, i.e. the principal of the connection
     *
     * It is the owner itself for a token a principal issued for itself, and the caller for a token of KIP-373
     * that was issued for somebody else. Below the version 3 the field is not on the wire and
     * {@see self::requester()} answers the owner.
     *
     * @since Version 3 of protocol (Kafka 3.3, KIP-373)
     */
    public KafkaPrincipal $tokenRequester;

    /**
     * Milliseconds since the epoch at which the broker issued the token
     */
    public int $issueTimestamp = self::ERROR_TIMESTAMP;

    /**
     * Milliseconds since the epoch at which the token expires unless it is renewed before
     */
    public int $expiryTimestamp = self::ERROR_TIMESTAMP;

    /**
     * Milliseconds since the epoch beyond which no renewal can move the expiry
     */
    public int $maxTimestamp = self::ERROR_TIMESTAMP;

    /**
     * Identifier of the token, a base64 uuid; it is the user name of a SASL/SCRAM login with this token
     */
    public string $tokenId = '';

    /**
     * Raw bytes of the HMAC of the token, the secret half of it; base64 of it is the password of that login
     */
    public string $hmac = '';

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Returns the principal that asked for the token, which is the owner below the version 3
     *
     * A version below 3 does not carry the field at all, so the property stays uninitialized there and the owner
     * is the only answer the frame has.
     */
    public function requester(): KafkaPrincipal
    {
        return $this->tokenRequester ?? $this->owner;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'errorCode'       => BinarySchema::TYPE_INT16,
            // `PrincipalType` and `PrincipalName` are two ordinary fields of the answer, not a structure of the
            // specification, so they are inlined here and carry no tagged-field section of their own in v2
            'owner'           => new InlineStruct(KafkaPrincipal::class),
        ];
        // The requester of KIP-373 sits between the owner and the timestamps, as two more flat fields
        if (static::VERSION >= self::TOKEN_REQUESTER_VERSION) {
            $body['tokenRequester'] = new InlineStruct(KafkaPrincipal::class);
        }
        $body += [
            'issueTimestamp'  => BinarySchema::TYPE_INT64,
            'expiryTimestamp' => BinarySchema::TYPE_INT64,
            'maxTimestamp'    => BinarySchema::TYPE_INT64,
            'tokenId'         => BinarySchema::TYPE_STRING,
            'hmac'            => BinarySchema::TYPE_BYTEARRAY,
            'throttleTimeMs'  => BinarySchema::TYPE_INT32,
        ];

        return $header + $body;
    }
}
