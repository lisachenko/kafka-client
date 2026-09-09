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
 * Asks a broker which api keys and versions it serves.
 *
 * The request has no body at all, only the common request header - the whole frame is 4 + 8 + the client id. It is
 * the first request a client of Kafka 0.10 or later sends on a new connection, and the only one whose answer tells a
 * client what the broker on the other side speaks; Kafka 0.9.0.1 and 0.8.2.2 have no such api, which is why the
 * lower lines of this repository probe the surface of their broker by hand.
 *
 * <pre>
 *   ApiVersionsRequest =>
 * </pre>
 *
 * Two properties of this api make it usable before anything else is known about the broker
 * (`KafkaApis.handleApiVersionsRequest` and `RequestChannel.Request` @ 0.10.2.2):
 *
 * * It is the **one** request whose unknown version does not cost the connection. Every other key or version a
 *   0.10.2.2 broker cannot parse makes it close the socket; an ApiVersions request of a version it does not serve is
 *   answered with the error code 35 (UnsupportedVersion) and an empty api array, in the format of **version 0**, so
 *   that a client which guessed too high can read the answer and retry with a lower version.
 * * It is answered on a SASL listener before the authentication has happened, so a client can learn the surface of
 *   the broker before it knows whether it may talk to it at all.
 *
 * @see docs/protocol/0.11.0.md, section "ApiVersions API (key 18, v0)"
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
    public const int VERSION = 0;

    /**
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }
}
