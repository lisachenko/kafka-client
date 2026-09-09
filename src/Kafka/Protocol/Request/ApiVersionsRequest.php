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

/**
 * Asks a broker which api keys and versions it serves, version 1 (key 18)
 *
 * The request has no body at all, only the common request header - the whole frame is 4 + 8 + the client id. It is
 * the first request a client of Kafka 0.10 or later sends on a new connection, and the only one whose answer tells a
 * client what the broker on the other side speaks; Kafka 0.9.0.1 and 0.8.2.2 have no such api, which is why the
 * lower lines of this repository probe the surface of their broker by hand.
 *
 * <pre>
 *   ApiVersions Request (Version: 1) =>
 * </pre>
 *
 * Kafka 0.11 raised the api to version 1, and `API_VERSIONS_REQUEST_V1 = API_VERSIONS_REQUEST_V0` in
 * `Protocol.java` @ 0.11.0.3 - the *request* is byte for byte the one of version 0, only the answer grew a
 * `throttle_time_ms` (KIP-124, {@see ApiVersionsResponse}). The version number on the wire is therefore the whole
 * difference between this class and {@see ApiVersionsRequestV0}, and it is what makes the broker answer the
 * version 1 layout.
 *
 * Two properties of this api make it usable before anything else is known about the broker
 * (`KafkaApis.handleApiVersionsRequest` and `RequestChannel.Request` @ 0.11.0.3):
 *
 * * It is the **one** request whose unknown version does not cost the connection. Every other key or version a
 *   0.11.0.3 broker cannot parse makes it close the socket; an ApiVersions request of a version it does not serve is
 *   answered with the error code 35 (UnsupportedVersion) and an empty api array, in the format of **version 0** -
 *   `ApiVersionsResponse.unsupportedVersionSend()` writes the answer with the version `0` no matter what was asked -
 *   so that a client which guessed too high can read the answer and retry with a lower version.
 * * It is answered on a SASL listener before the authentication has happened, so a client can learn the surface of
 *   the broker before it knows whether it may talk to it at all.
 *
 * @see docs/protocol/0.11.0.md, section "ApiVersions API (key 18, v0 and v1)"
 */
class ApiVersionsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::API_VERSIONS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }
}
