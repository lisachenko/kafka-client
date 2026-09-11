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
use Protocol\Kafka\Protocol\Data\FeatureUpdateKey;

/**
 * UpdateFeatures, version 0: raises or deletes a finalized feature (ApiKey 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   UpdateFeatures Request (Version: 0) => timeout_ms [feature_updates]
 *     timeout_ms      => INT32
 *     feature_updates => feature max_version_level allow_downgrade
 *       feature           => COMPACT_STRING
 *       max_version_level => INT16
 *       allow_downgrade   => BOOLEAN
 * </pre>
 *
 * The write half of KIP-584, whose read half is the **tagged fields of the ApiVersions v3 answer**
 * ({@see ApiVersionsResponse::$supportedFeatures}, `$finalizedFeatures` and `$finalizedFeaturesEpoch}): a feature
 * is a named version range that the *cluster* has agreed on, above the api versions that a single broker supports,
 * and this is how that agreement is changed.
 *
 * **Controller-only.** `KafkaApis` @ 2.8.2 answers the request on a broker that is not the active controller with
 * the top-level **41** (`NotController`), so {@see \Protocol\Kafka\Admin\AdminClient::updateFeatures()} sends it to
 * the controller and looks it up again once when it moved.
 *
 * A `max_version_level` **below 1** is not a version but the request to **delete** the finalized feature, and
 * every lowering - a deletion included - needs `allow_downgrade`.
 *
 * @see docs/protocol/2.8.md, section "UpdateFeatures API (key 57, v0)"
 */
class UpdateFeaturesRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::UPDATE_FEATURES;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * The `default` of `timeoutMs` in `UpdateFeaturesRequest.json` @ 2.8.2
     */
    public const int DEFAULT_TIMEOUT_MS = 60000;

    /**
     * Updates of this request, indexed by the feature name
     *
     * @var array<string, FeatureUpdateKey>
     */
    protected readonly array $featureUpdates;

    /**
     * @param array<string, FeatureUpdateKey> $featureUpdates Updates, by feature name
     * @param int                             $timeoutMs      How long the controller may take
     * @param string                          $clientId       A user specified identifier for the client
     * @param int                             $correlationId  A value the broker passes back unmodified
     */
    public function __construct(
        array $featureUpdates,
        protected readonly int $timeoutMs = self::DEFAULT_TIMEOUT_MS,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->featureUpdates = $featureUpdates;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'timeoutMs'      => BinarySchema::TYPE_INT32,
            'featureUpdates' => ['feature' => FeatureUpdateKey::class],
        ];
    }

    /**
     * Returns how long the controller may take over this request
     */
    public function getTimeoutMs(): int
    {
        return $this->timeoutMs;
    }
}
