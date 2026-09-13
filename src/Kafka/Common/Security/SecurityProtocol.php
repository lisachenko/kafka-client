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

namespace Protocol\Kafka\Common\Security;

/**
 * Possible values for the `security.protocol` configuration parameter
 *
 * Kafka 0.9.0.0 is the release that introduced pluggable listeners: a broker binds one listener per security
 * protocol (`listeners=PLAINTEXT://...,SSL://...,SASL_PLAINTEXT://...,SASL_SSL://...`) and every listener answers
 * the very same request set. The protocol is therefore a property of the transport, it never changes a single byte
 * of a request - the SASL listeners only prepend an authentication exchange to the connection.
 *
 * All four protocols are implemented on this branch. `SASL_PLAINTEXT` and `SASL_SSL` became implementable with
 * Kafka 0.10.0 (KIP-43), which added the `SaslHandshake` request (api key 17) and with it the PLAIN mechanism: in
 * 0.9 SASL meant Kerberos, negotiated *outside* the Kafka protocol, and PHP has no GSSAPI binding.
 * {@see \Protocol\Kafka\IO\SocketStream} performs the handshake and the token exchange of the PLAIN mechanism right
 * after the connection - and, for `SASL_SSL`, right after the TLS handshake - so that nothing above it notices.
 *
 * @see docs/protocol/2.8.md, section "Transport security (SSL)"
 * @see \Protocol\Kafka\Common\ClientConfig::SECURITY_PROTOCOL
 * @see \Protocol\Kafka\Common\Security\SaslMechanism for the mechanisms of the two SASL protocols
 */
final class SecurityProtocol
{
    /**
     * Un-encrypted, un-authenticated TCP - the only transport a Kafka 0.8.x broker speaks
     */
    public const string PLAINTEXT = 'PLAINTEXT';

    /**
     * TLS on top of TCP, with an optional client certificate
     */
    public const string SSL = 'SSL';

    /**
     * SASL authentication over plain TCP, i.e. credentials that travel in clear text with the PLAIN mechanism
     */
    public const string SASL_PLAINTEXT = 'SASL_PLAINTEXT';

    /**
     * SASL authentication inside a TLS channel, the only sound transport for the PLAIN mechanism
     */
    public const string SASL_SSL = 'SASL_SSL';

    /**
     * Returns the protocols this client can actually talk
     *
     * @return list<string>
     */
    public static function implemented(): array
    {
        return [self::PLAINTEXT, self::SSL, self::SASL_PLAINTEXT, self::SASL_SSL];
    }

    /**
     * Returns every protocol a Kafka 1.1.1 broker can bind a listener for
     *
     * @return list<string>
     */
    public static function all(): array
    {
        return [self::PLAINTEXT, self::SSL, self::SASL_PLAINTEXT, self::SASL_SSL];
    }

    /**
     * Tells whether the given protocol authenticates the client with SASL
     */
    public static function isSasl(string $securityProtocol): bool
    {
        return $securityProtocol === self::SASL_PLAINTEXT || $securityProtocol === self::SASL_SSL;
    }

    /**
     * Tells whether the given protocol encrypts the connection with TLS
     */
    public static function isEncrypted(string $securityProtocol): bool
    {
        return $securityProtocol === self::SSL || $securityProtocol === self::SASL_SSL;
    }
}
