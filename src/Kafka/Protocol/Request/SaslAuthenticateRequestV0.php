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
 * SaslAuthenticate request of version 0 (Kafka 1.0), the frame of version 1 with a lower version field
 *
 * <pre>
 *   SaslAuthenticate Request (Version: 0) => sasl_auth_bytes
 *     sasl_auth_bytes => BYTES
 * </pre>
 *
 * `SASL_AUTHENTICATE_REQUEST_V1 = SASL_AUTHENTICATE_REQUEST_V0` in `Protocol.java` @ 2.2.2: KIP-368 changed the
 * **answer** alone, which gained the `session_lifetime_ms` of a re-authenticating connection, so this class only
 * lowers the version constant of {@see SaslAuthenticateRequest}. A broker of the lines below this one serves the
 * version 0 and nothing else, and it is the version {@see \Protocol\Kafka\IO\SocketStream} sent until Kafka 2.2.
 *
 * @see docs/protocol/2.8.md, section "SaslAuthenticate API (key 36, v0 and v1)"
 */
final class SaslAuthenticateRequestV0 extends SaslAuthenticateRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
