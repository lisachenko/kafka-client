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
 * This request carries one SASL token of the negotiated mechanism to the broker.
 *
 * The api arrived with Kafka 1.0 (KIP-152, api key 36) and is the whole point of {@see SaslHandshakeRequest} v1: up
 * to Kafka 0.11 the tokens of the mechanism travelled as bare size-prefixed frames, with no header, no correlation
 * id and, above all, no way for the broker to report a refused credential - it closed the connection and the client
 * could not tell a wrong password from a dead broker. From Kafka 1.0 on the very same token bytes are the single
 * field of an ordinary request:
 *
 * <pre>
 *   SaslAuthenticate Request (Version: 0) => sasl_auth_bytes
 *     sasl_auth_bytes => bytes
 * </pre>
 *
 * and the answer ({@see SaslAuthenticateResponse}) carries an error code and a message.
 *
 * Two rules of `SaslServerAuthenticator` @ 1.1.1 govern when this request may be sent, and breaking either of them
 * costs the connection:
 *
 * * it is only understood **after a SaslHandshake v1**; after a v0 handshake the broker hands these very bytes to
 *   `SaslServer.evaluateResponse()` as if they were the token itself, the mechanism rejects them and the connection
 *   is closed without an answer;
 * * it belongs to the authentication phase alone. Sent before the handshake it is an "Unexpected Kafka request of
 *   type SASL_AUTHENTICATE during SASL handshake" and the connection is closed; sent *after* the authentication
 *   completed it reaches `KafkaApis.handleSaslAuthenticateRequest`, which answers the error code **34**
 *   (`IllegalSaslState`) and leaves the connection alive.
 *
 * For the PLAIN mechanism the exchange is a single request: the token is
 * {@see \Protocol\Kafka\Common\Security\SaslToken::ofPlainCredentials()}, and the answer is the empty token of a
 * completed exchange.
 *
 * @see docs/protocol/1.1.md, section "SaslAuthenticate API (key 36, v0)"
 * @see \Protocol\Kafka\IO\SocketStream::authenticate()
 */
class SaslAuthenticateRequest extends AbstractRequest
{
    /**
     * The only version a Kafka 1.1.1 broker serves; a version above it closes the connection without an answer
     */
    public const int VERSION = 0;

    public function __construct(
        /**
         * The SASL token of the negotiated mechanism, exactly the bytes a v0 exchange would put on the wire raw
         */
        protected readonly ?string $saslAuthBytes,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(ApiKeys::SASL_AUTHENTICATE, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'saslAuthBytes' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
