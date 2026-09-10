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
 * Possible values for the `sasl.mechanism` configuration parameter, i.e. the `Mechanism` of a SaslHandshake request.
 *
 * The names are the SASL mechanism names of the IANA registry, which is what the broker compares the `Mechanism`
 * field against (`sasl.enabled.mechanisms` of the broker, `GSSAPI` by default). A Kafka 0.10.2.2 broker can enable
 * `GSSAPI` (0.9), `PLAIN` (0.10.0, KIP-43) and the two SCRAM mechanisms (0.10.2, KIP-84).
 *
 * This client implements **PLAIN** only:
 *
 * * `GSSAPI` needs a Kerberos binding, and PHP has none in core - neither a GSS-API extension nor a pure-PHP
 *   implementation of the token exchange that a broker would accept;
 * * `SCRAM-SHA-256`/`SCRAM-SHA-512` need the multi-round SCRAM exchange of RFC 5802 with SASLprep-normalized
 *   credentials; the exchange is implementable in PHP, but it is a mechanism of its own and stays out of the scope
 *   of the SASL/PLAIN support of this client.
 *
 * PLAIN sends the credentials in clear text inside the SASL token, so it is only sound over `SASL_SSL`; the
 * `SASL_PLAINTEXT` listener exists for a trusted network and for tests.
 *
 * @see docs/protocol/1.1.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 * @see \Protocol\Kafka\Common\ClientConfig::SASL_MECHANISM
 */
final class SaslMechanism
{
    /**
     * Username and password in a single token, RFC 4616 (Kafka 0.10.0, KIP-43)
     */
    public const string PLAIN = 'PLAIN';

    /**
     * Kerberos v5 - not implemented, see the class docblock
     */
    public const string GSSAPI = 'GSSAPI';

    /**
     * Salted challenge response with SHA-256, RFC 5802 (Kafka 0.10.2, KIP-84) - not implemented
     */
    public const string SCRAM_SHA_256 = 'SCRAM-SHA-256';

    /**
     * Salted challenge response with SHA-512, RFC 5802 (Kafka 0.10.2, KIP-84) - not implemented
     */
    public const string SCRAM_SHA_512 = 'SCRAM-SHA-512';

    /**
     * Returns the mechanisms this client can actually perform
     *
     * @return list<string>
     */
    public static function implemented(): array
    {
        return [self::PLAIN];
    }

    /**
     * Returns every mechanism a Kafka 0.10.2.2 broker can enable
     *
     * @return list<string>
     */
    public static function all(): array
    {
        return [self::GSSAPI, self::PLAIN, self::SCRAM_SHA_256, self::SCRAM_SHA_512];
    }
}
