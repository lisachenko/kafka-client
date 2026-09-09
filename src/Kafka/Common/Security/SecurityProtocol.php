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
 * protocol (`listeners=PLAINTEXT://...,SSL://...`) and every listener answers the very same request set. The
 * protocol is therefore purely a property of the transport, it never changes a single byte of a request.
 *
 * This client implements PLAINTEXT and SSL. The two SASL protocols are rejected by {@see \Protocol\Kafka\IO\SocketStream}:
 * SASL in 0.9 is Kerberos (GSSAPI) only and is negotiated *outside* the Kafka protocol - the broker expects the raw
 * GSSAPI token exchange on a freshly opened connection, and the `SaslHandshake` request that made the mechanism
 * negotiable only arrived with Kafka 0.10.0 (api key 17).
 *
 * @see docs/protocol/0.10.2.md, section "Transport security (SSL)"
 * @see \Protocol\Kafka\Common\ClientConfig::SECURITY_PROTOCOL
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
     * SASL over plain TCP - not implemented, see the class docblock
     */
    public const string SASL_PLAINTEXT = 'SASL_PLAINTEXT';

    /**
     * SASL over TLS - not implemented, see the class docblock
     */
    public const string SASL_SSL = 'SASL_SSL';

    /**
     * Returns the protocols this client can actually talk
     *
     * @return list<string>
     */
    public static function implemented(): array
    {
        return [self::PLAINTEXT, self::SSL];
    }

    /**
     * Returns every protocol a Kafka 0.9.0.1 broker can bind a listener for
     *
     * @return list<string>
     */
    public static function all(): array
    {
        return [self::PLAINTEXT, self::SSL, self::SASL_PLAINTEXT, self::SASL_SSL];
    }
}
