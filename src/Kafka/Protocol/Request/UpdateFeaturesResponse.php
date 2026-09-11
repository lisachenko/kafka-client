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
 * UpdateFeatures response object, version 0 (key 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   UpdateFeatures Response (Version: 0) => throttle_time_ms error_code error_message [results]
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
 * @see docs/protocol/2.8.md, section "UpdateFeatures API (key 57, v0)"
 */
class UpdateFeaturesResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

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
