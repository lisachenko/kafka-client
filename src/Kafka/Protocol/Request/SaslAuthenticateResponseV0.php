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

/**
 * SaslAuthenticate response of version 0 (Kafka 1.0), the answer without the session lifetime of KIP-368
 *
 * <pre>
 *   SaslAuthenticate Response (Version: 0) => error_code error_message sasl_auth_bytes
 *     error_code      => INT16
 *     error_message   => NULLABLE_STRING
 *     sasl_auth_bytes => BYTES
 * </pre>
 *
 * Kafka 2.2 appended the `session_lifetime_ms` int64 of KIP-368 to this frame (version 1), so this class only
 * lowers the version constant that {@see SaslAuthenticateResponse::getScheme()} follows. An answer of this version
 * carries no lifetime at all, which is the "the connection never has to re-authenticate" of every broker below
 * Kafka 2.2.
 *
 * @see docs/protocol/2.8.md, section "SaslAuthenticate API (key 36, v0 to v2)"
 */
final class SaslAuthenticateResponseV0 extends SaslAuthenticateResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
