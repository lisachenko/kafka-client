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
use Protocol\Kafka\Protocol\Data\UpdatableFeatureResult;

/**
 * UpdateFeatures response object, version 2 (key 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   UpdateFeatures Response (Version: 0 to 2) => throttle_time_ms error_code error_message [results]
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => COMPACT_NULLABLE_STRING
 *     results          => feature error_code error_message  -- versions 0 and 1 only
 * </pre>
 *
 * **A top-level error answers no result at all**: 41 (`NotController`) when the request reached the wrong broker,
 * 42 (`InvalidRequest`) when the update list itself is malformed - a feature may not appear twice - and the whole
 * request is refused. Up to Kafka 3.9 a request the controller did look at was answered per feature, where **96**
 * (`FeatureUpdateFailed`) is what a ZooKeeper-backed 2.8.2 cluster says about every feature, because it finalizes
 * none.
 *
 * **Version 1 (KIP-778, Kafka 3.3) did not change these bytes**: `UpdateFeaturesResponse.json` @ 3.3.2 declares
 * every field `0+` and raises the `validVersions` to `0-1` alone, because the whole of KIP-778 is in the request
 * - the `upgrade_type` of an entry and the `validate_only` of the frame.
 *
 * **Version 2 (Kafka 4.0) drops the per-feature `results`**: "Version 2 changes the response to not return feature
 * level results" stands above the `validVersions` of the request @ 4.0.0, and the answer declares `Results` with
 * `"versions": "0-1"`. The reason is on the controller: `QuorumController.updateFeatures` @ 4.0.0 applies the
 * updates **atomically** - the first feature it refuses refuses the whole request, as the top-level error with the
 * message `The update failed for all features since the following feature had an error: …`, at every version - so
 * a list of results could only ever repeat the one top-level code. {@see UpdateFeaturesResponseV1} and
 * {@see UpdateFeaturesResponseV0} are the answers that still carry it; {@see self::$results} stays empty here.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 to v2)", "The upgrade type and the dry run
 *      of KIP-778 (v1)" and "The answer without results (v2, Kafka 4.0)"
 */
class UpdateFeaturesResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the request as a whole, 0 when every feature has a result of its own
     */
    public int $errorCode;

    /**
     * Human readable description of the top-level error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Result of every feature of the request, indexed by the feature name; always empty at the version 2
     *
     * @var array<string, UpdatableFeatureResult>
     */
    public array $results = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
        if (static::VERSION <= 1) {
            $body['results'] = ['feature' => UpdatableFeatureResult::class];
        }

        return $header + $body;
    }
}
