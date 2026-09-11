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
 * SaslAuthenticate response object, version 0 (key 36)
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
 * @see docs/protocol/2.8.md, section "SaslAuthenticate API (key 36, v0)"
 * @see \Protocol\Kafka\Common\Errors\SaslAuthenticationFailedException for the error code 58
 */
class SaslAuthenticateResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

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
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'     => BinarySchema::TYPE_INT16,
            'errorMessage'  => BinarySchema::TYPE_NULLABLE_STRING,
            'saslAuthBytes' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
