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
 * SaslAuthenticate response object, version 1 (key 36)
 *
 * <pre>
 *   SaslAuthenticate Response (Version: 0) => error_code error_message sasl_auth_bytes
 *     error_code      => INT16
 *     error_message   => NULLABLE_STRING
 *     sasl_auth_bytes => bytes
 * </pre>
 *
 * The api is Kafka 1.0 (KIP-152) and carries **no `throttle_time_ms`**: it is one of the four apis KIP-124 left
 * out, because it is answered before the connection is authenticated and therefore before a quota can be applied.
 *
 * Error codes a 1.1.1 broker reports here, all of them measured against `docker/kafka-2.8.2`:
 *
 * | Code | Name                     | Meaning                                                                    |
 * |------|--------------------------|----------------------------------------------------------------------------|
 * | 0    | None                     | The token was accepted; `errorMessage` is `null` and `saslAuthBytes` carries the answer of the mechanism - the empty token for a completed PLAIN exchange |
 * | 58   | SaslAuthenticationFailed | The credentials were refused: `Authentication failed: Invalid username or password` for a wrong password or an unknown user, `Authentication failed due to invalid credentials with SASL mechanism PLAIN` for a token the mechanism cannot even parse. The broker closes the connection right after this frame |
 * | 34   | IllegalSaslState         | `SaslAuthenticate request received after successful authentication` - the request reached `KafkaApis` instead of the authenticator; the connection stays usable for ordinary requests |
 *
 * An answer that carries an error code carries the **empty** `saslAuthBytes`, never a token.
 *
 * **Kafka 2.2 added the version 1** (KIP-368): a `session_lifetime_ms int64` behind the token, which says after
 * how many milliseconds the broker stops serving this connection unless the client has re-authenticated over it.
 * A broker that sets no `connections.max.reauth.ms` for the listener and no expiring credential answers **0**,
 * i.e. "the session never expires", which is what the container of this line does.
 * {@see SaslAuthenticateResponseV0} is the answer without that field.
 *
 * **Kafka 2.5 added the version 2** (KIP-482), the same fields in the flexible encoding: every string and array of
 * the frame is compact, the header carries a tag buffer and every structure ends in one. Not a field changed.
 *
 * @see docs/protocol/2.8.md, section "SaslAuthenticate API (key 36, v0 to v2)"
 * @see \Protocol\Kafka\Common\Errors\SaslAuthenticationFailedException for the error code 58
 */
class SaslAuthenticateResponse extends AbstractResponse
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
     * Error code of the answer, 0 when the broker accepted the token
     */
    public int $errorCode = 0;

    /**
     * Message of the broker for a failed authentication, `null` in a successful answer
     */
    public ?string $errorMessage = null;

    /**
     * Answer of the mechanism, an empty string both for a completed PLAIN exchange and for a failed one
     */
    public ?string $saslAuthBytes = '';

    /**
     * Milliseconds after which the broker refuses to serve this connection unless it has re-authenticated
     *
     * The `session_lifetime_ms` of KIP-368, and **0** when the session never expires - which is what a broker
     * without a `connections.max.reauth.ms` for this listener and mechanism answers, and what the container of
     * this line does. `SaslServerAuthenticator.calcCompletionTimesAndReturnSessionLifetimeMs()` @ 2.8.2 takes the
     * smaller of that option and the remaining lifetime of the credential itself (a delegation token or an
     * OAUTHBEARER token has one, SASL/PLAIN has not).
     *
     * A client with a positive lifetime re-authenticates on the same connection after a random 85 to 95 percent of
     * it, with a fresh SaslHandshake and SaslAuthenticate pair; this client does not do that yet (it is the
     * SaslAuthenticate v2 of Kafka 2.5), it only reads the value.
     *
     * @since Version 1 of protocol
     */
    public int $sessionLifetimeMs = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        $body = [
            'errorCode'     => BinarySchema::TYPE_INT16,
            'errorMessage'  => BinarySchema::TYPE_NULLABLE_STRING,
            'saslAuthBytes' => BinarySchema::TYPE_BYTEARRAY,
        ];
        if (static::VERSION >= 1) {
            $body['sessionLifetimeMs'] = BinarySchema::TYPE_INT64;
        }

        return $header + $body;
    }
}
