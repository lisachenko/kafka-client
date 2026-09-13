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
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Asks a broker which api keys and versions it serves, version 3 (key 18)
 *
 * It is the first request a client of Kafka 0.10 or later sends on a new connection, and the only one whose answer
 * tells a client what the broker on the other side speaks; Kafka 0.9.0.1 and 0.8.2.2 have no such api, which is why
 * the lower lines of this repository probe the surface of their broker by hand.
 *
 * <pre>
 *   ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER
 *     client_software_name    => COMPACT_STRING
 *     client_software_version => COMPACT_STRING
 * </pre>
 *
 * The versions 0, 1 and 2 have **no body at all** - `ApiVersionsRequest.json` @ 2.8.2 says "Versions 0 through 2 of
 * ApiVersionsRequest are the same" - so the whole frame of those is 4 + 8 + the client id, and the version number is
 * the entire message. Kafka 0.11 added the v1 whose *answer* carries a `throttle_time_ms` (KIP-124,
 * {@see ApiVersionsResponse}), and Kafka 2.0 the v2 of **KIP-219**: a client that sends it promises to honour the
 * throttle time itself, because a broker that throttles a v2 request answers **first** and mutes the channel
 * afterwards.
 *
 * **Version 3 is the first flexible version of the protocol this client speaks** (Kafka 2.4): its frame carries the
 * request header **v2** with a tagged-field section, its two new fields are **compact** strings and its body ends in
 * a tag buffer of its own. The two fields are KIP-511: they name the client *software*, not the connection, so that
 * a broker can report `kafka.server:type=ClientMetrics` per client library and version instead of guessing from the
 * client id. They are **not** the client id: `ClientConfig::CLIENT_ID` identifies the application and may be
 * anything, while these two are matched against `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`
 * (`ApiVersionsRequest.isValid()` @ 2.8.2) and a frame that fails that test is answered with the error code **42**
 * (`InvalidRequest`) - measured on the container, with an empty name, an empty version, a name with a `/` and a name
 * that starts with a `-`. This client sends {@see self::CLIENT_SOFTWARE_NAME} and
 * {@see self::CLIENT_SOFTWARE_VERSION}.
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
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
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
    public const int VERSION = 3;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Name of the client software this package is, as KIP-511 means it: the library, not the application
     *
     * The broker matches it against `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?` and counts one metric per name and
     * version, so it is the name of the Composer package with the `/` that the pattern refuses replaced by a `-`.
     */
    public const string CLIENT_SOFTWARE_NAME = 'lisachenko-kafka-client';

    /**
     * Version of the client software, which for this package is the Kafka protocol line it speaks
     *
     * Every line of this repository follows the Apache Kafka release it implements instead of a semantic version of
     * its own - `main` speaks Kafka 2.8 - so that is what a broker is told.
     */
    public const string CLIENT_SOFTWARE_VERSION = '2.8';

    /**
     * Name of the client software, sent from version 3 on (COMPACT_STRING, KIP-511)
     *
     * @since Version 3 of protocol
     */
    protected string $clientSoftwareName = self::CLIENT_SOFTWARE_NAME;

    /**
     * Version of the client software, sent from version 3 on (COMPACT_STRING, KIP-511)
     *
     * @since Version 3 of protocol
     */
    protected string $clientSoftwareVersion = self::CLIENT_SOFTWARE_VERSION;

    /**
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        if (static::VERSION < 3) {
            return $header;
        }

        return $header + [
            'clientSoftwareName'    => BinarySchema::TYPE_STRING,
            'clientSoftwareVersion' => BinarySchema::TYPE_STRING,
        ];
    }
}
