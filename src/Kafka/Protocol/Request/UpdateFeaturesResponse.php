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
 * UpdateFeatures response object, version 1 (key 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   UpdateFeatures Response (Version: 0 to 1) => throttle_time_ms error_code error_message [results]
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => COMPACT_NULLABLE_STRING
 *     results          => feature error_code error_message
 * </pre>
 *
 * **A top-level error answers no result at all**: 41 (`NotController`) when the request reached the wrong broker,
 * 42 (`InvalidRequest`) when the update list itself is malformed - it may not be empty, and a feature may not
 * appear twice - and the whole request is refused. A request the controller did look at is answered per feature,
 * where **96** (`FeatureUpdateFailed`) is what a ZooKeeper-backed 2.8.2 cluster says about every feature, because
 * it finalizes none.
 *
 * **Version 1 (KIP-778, Kafka 3.3) did not change these bytes**: `UpdateFeaturesResponse.json` @ 3.3.2 declares
 * every field `0+` and raises the `validVersions` to `0-1` alone, because the whole of KIP-778 is in the request
 * - the `upgrade_type` of an entry and the `validate_only` of the frame. What separates the two answers is
 * therefore only the question they answer: an answer of this version may be the result of a **dry run**, i.e. of
 * a request the controller looked at and wrote nothing for. {@see UpdateFeaturesResponseV0} is the same shape one
 * api version lower.
 *
 * @see docs/protocol/3.9.md, sections "UpdateFeatures API (key 57, v0 and v1)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
class UpdateFeaturesResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

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
     * Result of every feature of the request, indexed by the feature name
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

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'results'        => ['feature' => UpdatableFeatureResult::class],
        ];
    }
}
