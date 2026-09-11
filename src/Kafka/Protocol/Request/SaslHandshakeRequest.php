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
 * This request queries the supported SASL mechanisms on the broker and selects the one the client will use.
 *
 * The api arrived with Kafka 0.10.0 (KIP-43, api key 17): before it, a `SASL_PLAINTEXT`/`SASL_SSL` listener expected
 * the raw GSSAPI token exchange on a freshly opened connection, with no framing of the Kafka protocol around it and
 * no way to negotiate the mechanism. It is - together with `ApiVersions` - one of the two requests a broker answers
 * on such a listener before the authentication completed, and it is answered exactly once per connection.
 *
 * <pre>
 *   SaslHandshakeRequest => Mechanism
 *     Mechanism => string
 * </pre>
 *
 * **Version 1 (Kafka 1.0, KIP-152) is the very same frame in both directions**
 * (`SASL_HANDSHAKE_REQUEST_V1 = SASL_HANDSHAKE_REQUEST_V0` in `SaslHandshakeRequest.java` @ 1.1.1); what the version
 * changes is the *promise* the client makes about everything that follows the handshake:
 *
 * * **v0** - the tokens of the mechanism travel as bare size-prefixed frames without a header of their own, and a
 *   broker that refuses the credentials has no way to say so: it closes the connection, see
 *   {@see SaslHandshakeRequestV0};
 * * **v1** - the tokens travel inside {@see SaslAuthenticateRequest} (api key 36), an ordinary request with a header,
 *   and the answer carries an error code and a message, so a wrong password is the error code **58**
 *   (`SaslAuthenticationFailed`) instead of a dropped socket.
 *
 * `SaslServerAuthenticator.handleHandshakeRequest()` @ 1.1.1 makes that switch on the version alone
 * (`if (version >= 1) this.enableKafkaSaslAuthenticateHeaders(true)`), so the two halves must match: a raw token
 * after a v1 handshake, or a framed request after a v0 one, is read as garbage and the connection is closed.
 *
 * @see docs/protocol/2.8.md, section "SaslHandshake API (key 17, v0 and v1)"
 * @see \Protocol\Kafka\IO\SocketStream::authenticate() for the exchange this request opens
 */
class SaslHandshakeRequest extends AbstractRequest
{
    /**
     * The highest version a Kafka 1.1.1 broker serves, and the one this client sends (Kafka 1.0, KIP-152)
     */
    public const int VERSION = 1;

    public function __construct(
        /**
         * SASL mechanism chosen by the client, one of the {@see \Protocol\Kafka\Common\Security\SaslMechanism} values
         */
        protected readonly string $mechanism,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(ApiKeys::SASL_HANDSHAKE, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'mechanism' => BinarySchema::TYPE_STRING,
        ];
    }
}
