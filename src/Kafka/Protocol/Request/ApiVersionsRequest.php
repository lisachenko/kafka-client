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
 * Asks a broker which api keys and versions it serves, version 5 (key 18)
 *
 * It is the first request a client of Kafka 0.10 or later sends on a new connection, and the only one whose answer
 * tells a client what the broker on the other side speaks; Kafka 0.9.0.1 and 0.8.2.2 have no such api, which is why
 * the lower lines of this repository probe the surface of their broker by hand.
 *
 * <pre>
 *   ApiVersions Request (Version: 3 to 4) => client_software_name client_software_version TAG_BUFFER
 *     client_software_name    => COMPACT_STRING
 *     client_software_version => COMPACT_STRING
 *
 *   ApiVersions Request (Version: 5) => client_software_name client_software_version cluster_id node_id TAG_BUFFER
 *     cluster_id => COMPACT_NULLABLE_STRING
 *     node_id    => INT32
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
 * **Version 4 (Kafka 3.9) declares no field at all.** Its comment in `ApiVersionsRequest.json` @ 3.9.2 is the
 * whole specification of it - *"Version 4 fixes KAFKA-17011, which blocked SupportedFeatures.MinVersion in the
 * response from being 0"* - so the frame of a v4 request is the frame of a v3 request with a 4 in its header, and
 * what the version really is is a **promise about the answer**: a client that sends it can read a supported
 * feature whose `min_version` is **0**. A v3 answer hides such a feature altogether
 * (`ApiVersionsResponse.maybeFilterSupportedFeatureKeys` @ 3.9.2 drops every entry with the minimum 0, because the
 * clients of Kafka 3.0 to 3.8 read a 0 there as "no such feature"), and on the KRaft node of this line that is
 * `kraft.version`: the v3 answer lists one supported feature, the v4 answer lists two ({@see ApiVersionsResponse}).
 * {@see ApiVersionsRequestV3} is the frame one number below it.
 *
 * **Version 5 (Kafka 4.4, KIP-1242) lets the client say whom it means to reach.** "Version 5 introduces ClusterId
 * and NodeId checking and REBOOTSTRAP_REQUIRED error" (`ApiVersionsRequest.json` @ 4.4.0) appends two non-tagged
 * fields behind the KIP-511 strings: `cluster_id` (a compact *nullable* string, default null) and `node_id` (int32,
 * default -1), "the cluster ID / node ID the client intends to connect to. Provide both ClusterId and NodeId, if
 * known." The two travel **together or not at all**: `ApiVersionsRequest.isValid()` @ 4.4.0 refuses a version 5
 * whose cluster id is null next to a node id other than -1, or set next to the node id -1, and the broker answers
 * such a half pair with the **42** `InvalidRequest` of the KIP-511 pattern check - measured on the 4.4.0 node for
 * both halves, see {@see self::addressedTo()}. A valid pair is checked next (`KafkaApis.handleApiVersionsRequest`
 * @ 4.4.0, behind the 35 of an unknown version and the 42): a cluster id that is not this cluster's, or the right
 * cluster id and another node id, is answered with the error code **129** `REBOOTSTRAP_REQUIRED`, an empty api array
 * and the layout of the version asked for - the connection reached a broker the client did not mean (an address
 * that now belongs to another node, a load balancer in front of another cluster), so the metadata of the client is
 * stale. The null cluster id with the node id -1 switches the check off, which is the frame of a client that knows
 * neither. The answer is the v4 answer: the version changes no field of it. This client sends the version 5, and
 * {@see ApiVersionsRequestV4} is the frame one number below it.
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
 * @see docs/protocol/overview.md, section "ApiVersions API (key 18, v0 to v5)"
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
    public const int VERSION = 5;

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
     * its own - `main` speaks Kafka 4.4 - so that is what a broker is told.
     */
    public const string CLIENT_SOFTWARE_VERSION = '4.4';

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
     * Cluster the client intends to connect to, null when it does not know it (COMPACT_NULLABLE_STRING, KIP-1242)
     *
     * A null - next to the node id -1 - switches the check of the broker off; any other value is compared with the
     * cluster id of the broker, and {@see self::$nodeId} next to it with its node id.
     *
     * @since Version 5 of protocol
     */
    protected ?string $clusterId = null;

    /**
     * Node the client intends to connect to, -1 when it does not know it (INT32, KIP-1242)
     *
     * It is -1 exactly when the cluster id is null, or the broker answers the 42; the broker answers the 129 when the
     * two do not name the broker itself.
     *
     * @since Version 5 of protocol
     */
    protected int $nodeId = -1;

    /**
     * @param string      $clientId      A user specified identifier for the client making the request
     * @param int         $correlationId A user-supplied value that the broker passes back unmodified
     * @param string|null $clusterId     Cluster the client means to reach (version 5, KIP-1242), null if unknown
     * @param int         $nodeId        Node the client means to reach (version 5, KIP-1242), -1 if unknown
     */
    public function __construct(string $clientId = '', int $correlationId = 0, ?string $clusterId = null, int $nodeId = -1)
    {
        // The parent computes the size of the frame, so every field of the body has to be set before it runs
        $this->clusterId = $clusterId;
        $this->nodeId    = $nodeId;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the request for one known node of one known cluster, with both ids or with neither (KIP-1242)
     *
     * `NetworkClient` @ 4.4.0 fills the two fields only when it knows the cluster id from its metadata **and** the
     * node id is not negative, because the broker refuses half a pair - a cluster id next to the node id -1, or a
     * node id next to the null cluster id - with the 42 (`ApiVersionsRequest.isValid()` @ 4.4.0): half of the pair
     * is sent as none of it.
     *
     * @param string|null $clusterId     Cluster id of the metadata of the client, null while it knows none
     * @param int         $nodeId        Id of the node the connection is meant to reach, negative while unknown
     * @param string      $clientId      A user specified identifier for the client making the request
     * @param int         $correlationId A user-supplied value that the broker passes back unmodified
     */
    public static function addressedTo(?string $clusterId, int $nodeId, string $clientId = '', int $correlationId = 0): self
    {
        if ($clusterId === null || $nodeId < 0) {
            return new self($clientId, $correlationId);
        }

        return new self($clientId, $correlationId, $clusterId, $nodeId);
    }

    /**
     * Returns the cluster id this request names, null when it names none
     */
    public function getClusterId(): ?string
    {
        return $this->clusterId;
    }

    /**
     * Returns the node id this request names, -1 when it names none
     */
    public function getNodeId(): int
    {
        return $this->nodeId;
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

        $body = [
            'clientSoftwareName'    => BinarySchema::TYPE_STRING,
            'clientSoftwareVersion' => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 5) {
            $body['clusterId'] = BinarySchema::TYPE_NULLABLE_STRING;
            $body['nodeId']    = BinarySchema::TYPE_INT32;
        }

        return $header + $body;
    }
}
