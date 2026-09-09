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
 * Possible values for the `ssl.protocol` and `ssl.enabled.protocols` configuration parameters
 *
 * The names are the ones the Java client uses for `SSLContext.getInstance()`, so that a configuration can be moved
 * between the two clients unchanged; {@see \Protocol\Kafka\IO\SocketStream} maps them onto the `STREAM_CRYPTO_METHOD_*`
 * constants of PHP.
 *
 * `TLS` means "whatever TLS version both sides support" and is the default, exactly as in the Java client. The
 * broker of Kafka 0.10.2.2 offers TLSv1, TLSv1.1 and TLSv1.2 (`ssl.enabled.protocols`), so `TLS` negotiates TLSv1.2
 * against a default-configured 0.9 broker.
 *
 * The SSLv2/SSLv3 members exist because the Java client accepts them; they are broken protocols and modern OpenSSL
 * builds refuse them outright, which surfaces here as a failed handshake.
 *
 * @see docs/protocol/0.11.0.md, section "Transport security (SSL)"
 * @see \Protocol\Kafka\Common\ClientConfig::SSL_PROTOCOL
 */
final class SslProtocol
{
    /**
     * Any TLS version supported by both peers
     */
    public const string TLS = 'TLS';

    /**
     * TLS 1.1 only
     */
    public const string TLSv1_1 = 'TLSv1_1';

    /**
     * TLS 1.2 only
     */
    public const string TLSv1_2 = 'TLSv1_2';

    /**
     * Any SSL/TLS version supported by both peers (the SSLv23 handshake of OpenSSL)
     */
    public const string SSL = 'SSL';

    /**
     * SSL 2.0 only - broken, refused by current OpenSSL builds
     */
    public const string SSLv2 = 'SSLv2';

    /**
     * SSL 3.0 only - broken, refused by current OpenSSL builds
     */
    public const string SSLv3 = 'SSLv3';
}
