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
 * Asks a broker which api keys and versions it serves, version 2 (key 18)
 *
 * The request has no body at all up to and including this version, only the common request header - the whole frame
 * is 4 + 8 + the client id. It is the first request a client of Kafka 0.10 or later sends on a new connection, and
 * the only one whose answer tells a client what the broker on the other side speaks; Kafka 0.9.0.1 and 0.8.2.2 have
 * no such api, which is why the lower lines of this repository probe the surface of their broker by hand.
 *
 * <pre>
 *   ApiVersions Request (Version: 2) =>
 * </pre>
 *
 * `ApiVersionsRequest.json` @ 2.8.2 says "Versions 0 through 2 of ApiVersionsRequest are the same": the three
 * frames differ in exactly one field, the `ApiVersion` of the request header, and the version number is the whole
 * message. Kafka 0.11 added the v1 whose *answer* carries a `throttle_time_ms` (KIP-124,
 * {@see ApiVersionsResponse}), and Kafka **2.0** added this v2 as one of the api bumps of **KIP-219**: a client
 * that sends it promises to honour the throttle time itself, because a broker that throttles a v2 request answers
 * **first** and mutes the channel afterwards, where it used to mute the channel and answer at the end of the
 * throttle window. Nothing on the wire changes - `ApiVersionsResponse.json` @ 2.8.2 notes it as "Starting in
 * version 2, on quota violation, brokers send out responses before throttling".
 *
 * Kafka 2.4 raised the api once more, to the first **flexible** version 3 (KIP-482 and KIP-511: a compact
 * `client_software_name` and `client_software_version`, a request header v2 and tagged fields); that version is not
 * implemented on this branch yet, and a 2.8.2 broker answers it as it answers every version above the one this
 * class sends - see {@see ApiVersionsResponse} for what comes back.
 *
 * Two properties of this api make it usable before anything else is known about the broker
 * (`KafkaApis.handleApiVersionsRequest` and `RequestContext.parseRequest` @ 2.8.2):
 *
 * * It is the **one** request whose unknown version does not cost the connection. Every other key or version a
 *   2.8.2 broker cannot parse makes it close the socket; an ApiVersions request of a version it does not serve is
 *   answered with the error code 35 (UnsupportedVersion), in the format of **version 0** - `RequestContext` rebuilds
 *   the request as an `ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0, header.apiVersion())` and never
 *   reads the body - so that a client which guessed too high can read the answer and retry with a lower version.
 *   Since Kafka 2.4 (KIP-511) that answer is not empty any more: it carries the one row of the ApiVersions api
 *   itself, so the client learns which version to ask for. **The header of such a request still has to be the one
 *   the requested version prescribes** - `RequestHeader.parse` @ 2.8.2 derives the header version from the
 *   *requested* api version, so a v4 frame with the plain header of this class is a parse failure and costs the
 *   connection, while a v4 frame with a request header v2 is answered with the 35.
 * * It is answered on a SASL listener before the authentication has happened, so a client can learn the surface of
 *   the broker before it knows whether it may talk to it at all.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v2)"
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
    public const int VERSION = 2;

    /**
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }
}
