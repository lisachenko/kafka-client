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
 * no way to negotiate the mechanism. It is the *only* request a broker answers on such a listener before the
 * authentication completed, and it is answered exactly once per connection.
 *
 * <pre>
 *   SaslHandshakeRequest => Mechanism
 *     Mechanism => string
 * </pre>
 *
 * A 0.10.2.2 broker serves version 0 of it and nothing else; the request that wraps the tokens themselves
 * (`SaslAuthenticate`, api key 36) is Kafka 1.0 and does not exist here - see the "SASL/PLAIN" section of the
 * protocol document for the framing that replaces it.
 *
 * @see docs/protocol/1.1.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 * @see \Protocol\Kafka\IO\SocketStream::authenticate() for the exchange this request opens
 */
class SaslHandshakeRequest extends AbstractRequest
{
    /**
     * The only version of the api that a Kafka 0.10.2.2 broker serves
     */
    public const int VERSION = 0;

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
