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
 * SaslHandshake, version 0: the request of Kafka 0.10.0 (key 17)
 *
 * The frame of version 1 and the frame of version 0 are the same bytes -
 * `SASL_HANDSHAKE_REQUEST_V1 = SASL_HANDSHAKE_REQUEST_V0` in `SaslHandshakeRequest.java` @ 1.1.1 - and so is the
 * answer; the only difference is the `ApiVersion` of the request header, and what the broker does with the tokens
 * that follow it. A **version 0** handshake leaves `SaslServerAuthenticator` @ 1.1.1 with
 * `enableKafkaSaslAuthenticateHeaders = false`, so the tokens are read as bare size-prefixed frames and a refused
 * credential is a closed connection, exactly as on every line up to 0.11 - see {@see SaslHandshakeRequest} for the
 * version 1 that wraps them into {@see SaslAuthenticateRequest} instead.
 *
 * This class is what a client sends to a broker of the lines up to `0.11.x`, which report the api as v0 only, and
 * it is the class the version 0 wire vectors are replayed through.
 *
 * @see docs/protocol/1.1.md, section "SaslHandshake API (key 17, v0 and v1)"
 */
final class SaslHandshakeRequestV0 extends SaslHandshakeRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
