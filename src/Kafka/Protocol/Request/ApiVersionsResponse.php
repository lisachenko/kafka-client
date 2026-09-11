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
 * The api keys and versions one broker serves, version 2 (key 18)
 *
 * <pre>
 *   ApiVersions Response (Version: 2) => error_code [api_versions] throttle_time_ms
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
 * every key of its listener, including the broker-to-broker apis 4, 5, 6, 27 and 56 that no client ever sends.
 *
 * **The frame of version 2 is the frame of version 1**, byte for byte - `ApiVersionsResponse.json` @ 2.8.2 adds no
 * field to it and only notes "Starting in version 2, on quota violation, brokers send out responses before
 * throttling", which is the KIP-219 promise of Kafka 2.0 that the client honours the throttle time itself.
 *
 * **`throttle_time_ms` is the LAST field of this answer, not the first.** Kafka 0.11 added the field to fifteen
 * apis with KIP-124 and put it in front of the body everywhere else - Metadata v3, OffsetCommit v3, JoinGroup v2 and
 * the rest - but the ApiVersions answer appends it, because a client that guessed the version wrong has to be able
 * to read the leading `error_code` of the version 0 layout out of it. It is 0 unless a `request_percentage` quota
 * throttled the connection, which an ApiVersions request practically never does.
 *
 * `ErrorCode` belongs to the whole answer and is 0 or **35** (UnsupportedVersion, `Errors.java` @ 2.8.2). The
 * second case is the answer to an ApiVersions request of a version the broker does not serve: the frame is then the
 * **version 0** layout - without the throttle time - no matter which version was asked for, because
 * `RequestContext.parseRequest` @ 2.8.2 rebuilds the request as a version 0 one. Since Kafka 2.4 (KIP-511) that
 * answer carries **one** api row, the range of ApiVersions itself (`ApiVersionsRequest.getErrorResponse()`), where
 * a 1.1.1 broker answered an empty array; a 2.8.2 broker therefore tells a client which version it should have
 * asked for. That answer is read with {@see ApiVersionsResponseV0}, and the connection survives it, which no other
 * unknown key or version of a 2.8.2 broker does.
 *
 * A 2.8.2 broker answers with the **56** keys 0 to 51, 56, 57, 60 and 61 - the `zkBroker` listener set of the JSON
 * message specifications. A client must not assume that, though - the whole point of the api is that the set is
 * whatever the broker on the other side reports, and a later broker reports more. The set is not even fixed for one
 * release: the answer is built from the apis of the **listener** the request arrived on (`ApiVersionManager` @
 * 2.8.2, KIP-500), so the KRaft apis 52-55, 58, 59 and 62-64 of the controller listener never appear here, and an
 * api whose `minRequiredInterBrokerMagic` is above the message format of the broker is dropped as well.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v2)"
 */
class ApiVersionsResponse extends AbstractResponse
{
    /**
     * Version of the ApiVersions API that this class decodes the answer of
     */
    public const int VERSION = 2;

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
