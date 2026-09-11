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
use Protocol\Kafka\Protocol\Data\ApiVersionsFinalizedFeature;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Data\ApiVersionsSupportedFeature;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * The api keys and versions one broker serves, version 3 (key 18)
 *
 * <pre>
 *   ApiVersions Response (Version: 3) => error_code [api_versions] throttle_time_ms TAG_BUFFER
 *     error_code    => INT16
 *     api_versions  => api_key min_version max_version TAG_BUFFER   (COMPACT_ARRAY)
 *       api_key     => INT16
 *       min_version => INT16
 *       max_version => INT16
 *     throttle_time_ms => INT32
 *     TAG_BUFFER    => 0: supported_features, 1: finalized_features_epoch, 2: finalized_features
 * </pre>
 *
 * **The answer of this api always carries a response header v0**, flexible body or not - the one exception that
 * `ApiKeys.responseHeaderVersion()` @ 2.8.2 writes out by hand, with the comment *"ApiVersionsResponse always
 * includes a v0 header. See KIP-511 for details."* A client that guessed the version too high has to be able to
 * read the correlation id and the error code of the answer without knowing which header the broker used, so the
 * header of this api was frozen when every other one grew a tag buffer.
 *
 * The array is indexed by the api key, so that a caller can ask for one api directly
 * ({@see self::supports()}, {@see self::maxVersionOf()}); the broker sends the keys in ascending order and reports
 * every key of its listener, including the broker-to-broker apis 4, 5, 6, 27 and 56 that no client ever sends.
 *
 * **The frame of version 2 is the frame of version 1**, byte for byte - `ApiVersionsResponse.json` @ 2.8.2 adds no
 * field to it and only notes "Starting in version 2, on quota violation, brokers send out responses before
 * throttling", which is the KIP-219 promise of Kafka 2.0 that the client honours the throttle time itself.
 *
 * **Version 3 is the first flexible one** (Kafka 2.4): the api array counts its entries compactly, every entry of it
 * ends in a tagged-field section of its own, and the body ends in one that carries the three optional fields of
 * KIP-584 - the features the broker supports (tag 0), the epoch of the finalized features (tag 1) and the finalized
 * features themselves (tag 2). A ZooKeeper-backed 2.8.2 broker answers **only tag 1**, with the value 0: it supports
 * no feature and has finalized none, and a tagged field is written only when its value differs from the default of
 * the specification, which for the epoch is -1.
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
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
class ApiVersionsResponse extends AbstractResponse
{
    /**
     * Version of the ApiVersions API that this class decodes the answer of
     */
    public const int VERSION = 3;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 3;

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
     * Features the broker supports, indexed by their name (tagged field 0, KIP-584)
     *
     * Empty unless the broker declares a feature, which a ZooKeeper-backed 2.8.2 broker never does: the tagged
     * field is then simply absent from the answer, which is what "empty" looks like on the wire.
     *
     * @var array<string, ApiVersionsSupportedFeature>
     *
     * @since Version 3 of protocol
     */
    public array $supportedFeatures = [];

    /**
     * Monotonically increasing epoch of the finalized features, `-1` when the broker knows none (tagged field 1)
     *
     * The default of the specification is -1, so a broker that answers **0** - which the container does, because the
     * ZooKeeper node that carries the finalized features starts at version 0 - really writes the field out.
     *
     * @since Version 3 of protocol
     */
    public int $finalizedFeaturesEpoch = -1;

    /**
     * Cluster-wide finalized features, indexed by their name (tagged field 2, KIP-584)
     *
     * Only meaningful while {@see self::$finalizedFeaturesEpoch} is `>= 0`.
     *
     * @var array<string, ApiVersionsFinalizedFeature>
     *
     * @since Version 3 of protocol
     */
    public array $finalizedFeatures = [];

    /**
     * The answer of this api is the one frame of the protocol whose header never became flexible (KIP-511)
     */
    public static function getHeaderVersion(): int
    {
        return self::HEADER_V0;
    }

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
        if (static::VERSION >= 3) {
            $body['supportedFeatures']      = new TaggedField(
                0,
                ['name' => ApiVersionsSupportedFeature::class],
                []
            );
            $body['finalizedFeaturesEpoch'] = new TaggedField(1, BinarySchema::TYPE_INT64, -1);
            $body['finalizedFeatures']      = new TaggedField(
                2,
                ['name' => ApiVersionsFinalizedFeature::class],
                []
            );
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
