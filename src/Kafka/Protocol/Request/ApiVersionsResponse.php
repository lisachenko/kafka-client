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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;

/**
 * The api keys and versions one broker serves, version 1 (key 18)
 *
 * <pre>
 *   ApiVersions Response (Version: 1) => error_code [api_versions] throttle_time_ms
 *     error_code    => INT16
 *     api_versions  => api_key min_version max_version
 *       api_key     => INT16
 *       min_version => INT16
 *       max_version => INT16
 *     throttle_time_ms => INT32
 * </pre>
 *
 * The array is indexed by the api key, so that a caller can ask for one api directly
 * ({@see self::supports()}, {@see self::maxVersionOf()}); the broker sends the keys in ascending order and reports
 * every key it knows, including the broker-to-broker apis 4, 5, 6 and 27 that no client ever sends.
 *
 * **`throttle_time_ms` is the LAST field of this answer, not the first.** Kafka 0.11 added the field to fifteen
 * apis with KIP-124 and put it in front of the body everywhere else - Metadata v3, OffsetCommit v3, JoinGroup v2 and
 * the rest - but `API_VERSIONS_RESPONSE_V1` in `Protocol.java` @ 0.11.0.3 appends it, because a client that guessed
 * the version wrong has to be able to read the leading `error_code` of the version 0 layout out of it. It is 0
 * unless a `request_percentage` quota throttled the connection, which an ApiVersions request practically never does.
 *
 * `ErrorCode` belongs to the whole answer and is 0 or **35** (UnsupportedVersion, `Errors.java` @ 0.11.0.3). The
 * second case is the answer to an ApiVersions request of a version the broker does not serve: the array is then
 * empty, and the frame is the **version 0** layout - without the throttle time - no matter which version was asked
 * for, because `ApiVersionsResponse.unsupportedVersionSend()` writes it with the hard-coded version `0`. That answer
 * is therefore read with {@see ApiVersionsResponseV0}, and the connection survives it, which no other unknown key or
 * version of a 1.1.1 broker does - not even ControlledShutdown, which answered every version up to 0.11.
 *
 * A 1.1.1 broker answers with the 43 keys 0 to 42; a client must not assume that, though - the whole point of the
 * api is that the set is whatever the broker on the other side reports, and a later broker reports more. The set is
 * not even fixed for one release: `ApiVersionsResponse.apiVersionsResponse()` @ 1.1.1 drops every api whose
 * `minRequiredInterBrokerMagic` is above the message format the broker runs with, so a 1.1 broker configured with
 * `inter.broker.protocol.version=0.10.2` reports fewer keys than the container of this repository does.
 *
 * @see docs/protocol/1.1.md, section "ApiVersions API (key 18, v0 and v1)"
 */
class ApiVersionsResponse extends AbstractResponse
{
    /**
     * Version of the ApiVersions API that this class decodes the answer of
     */
    public const int VERSION = 1;

    /**
     * Error code of the whole request, 0 or 35 (UnsupportedVersion)
     */
    public int $errorCode = 0;

    /**
     * Version range of every api the broker serves, indexed by the api key
     *
     * @var array<int, ApiVersionsResponseMetadata>
     */
    public array $apiVersions = [];

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * Unlike every other api of Kafka 0.11 this field closes the answer instead of opening it.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'errorCode'   => BinarySchema::TYPE_INT16,
            'apiVersions' => ['apiKey' => ApiVersionsResponseMetadata::class],
        ];
        if (static::VERSION >= 1) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }

        return $header + $body;
    }

    /**
     * Checks whether the broker serves the given version of an api
     *
     * @param int $apiKey  One of the constants of {@see \Protocol\Kafka\Protocol\ApiKeys}
     * @param int $version Version to look for
     */
    public function supports(int $apiKey, int $version): bool
    {
        return isset($this->apiVersions[$apiKey]) && $this->apiVersions[$apiKey]->supports($version);
    }

    /**
     * Returns the highest version of an api the broker serves, or null when it does not serve that api at all
     *
     * @param int $apiKey One of the constants of {@see \Protocol\Kafka\Protocol\ApiKeys}
     */
    public function maxVersionOf(int $apiKey): ?int
    {
        return $this->apiVersions[$apiKey]->maxVersion ?? null;
    }
}
