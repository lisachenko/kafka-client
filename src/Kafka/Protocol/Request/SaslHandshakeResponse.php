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
 * SASL handshake response
 *
 * <pre>
 *   SaslHandshakeResponse => ErrorCode [EnabledMechanisms]
 *     ErrorCode         => int16
 *     EnabledMechanisms => string
 * </pre>
 *
 * The frame is the same for version 0 and version 1 of the api - `SASL_HANDSHAKE_RESPONSE_V1 =
 * SASL_HANDSHAKE_RESPONSE_V0` in `SaslHandshakeResponse.java` @ 1.1.1 - so this one class reads both, and only the
 * request tells the broker which exchange follows, see {@see SaslHandshakeRequest}.
 *
 * The error code is 0 when the broker accepted the mechanism of the request, and 33 (UnsupportedSaslMechanism) when
 * it did not; in both of those `EnabledMechanisms` carries the `sasl.enabled.mechanisms` of the broker, so a client
 * can report what it could have asked for. After an error the broker closes the connection instead of waiting for
 * another handshake (`SaslServerAuthenticator.handleHandshakeRequest` @ 1.1.1).
 *
 * A **34** (IllegalSaslState) is the third answer, and the only one with an *empty* mechanism list: it means the
 * handshake did not reach the authenticator at all - `KafkaApis.handleSaslHandshakeRequest` @ 1.1.1 answers
 * `new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE, Collections.emptySet())` for a handshake on a listener that
 * does not authenticate, and the authenticator answers the same frame for a second handshake on a connection that
 * already has one. (Kafka 1.0.2 still filled that answer with `config.saslEnabledMechanisms`; 1.1 empties it.)
 *
 * @see docs/protocol/1.1.md, section "SaslHandshake API (key 17, v0 and v1)"
 */
class SaslHandshakeResponse extends AbstractResponse
{
    /**
     * Error code.
     */
    public int $errorCode;

    /**
     * Array of mechanisms enabled in the server.
     *
     * @var list<string>
     */
    public array $enabledMechanisms = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'         => BinarySchema::TYPE_INT16,
            'enabledMechanisms' => [BinarySchema::TYPE_STRING],
        ];
    }
}
