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
use Protocol\Kafka\Protocol\Data\FeatureUpdateKeyV0;

/**
 * UpdateFeatures, version 2: raises, lowers or deletes a finalized feature (ApiKey 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   UpdateFeatures Request (Version: 0 to 2) => timeout_ms [feature_updates] validate_only
 *     timeout_ms      => INT32
 *     feature_updates => feature max_version_level allow_downgrade|upgrade_type
 *       feature           => COMPACT_STRING
 *       max_version_level => INT16
 *       allow_downgrade   => BOOLEAN  -- version 0 only
 *       upgrade_type      => INT8     -- since version 1
 *     validate_only   => BOOLEAN      -- since version 1
 * </pre>
 *
 * The write half of KIP-584, whose read half is the **tagged fields of the ApiVersions v3 answer**
 * ({@see ApiVersionsResponse::$supportedFeatures}, `$finalizedFeatures` and `$finalizedFeaturesEpoch}): a feature
 * is a named version range that the *cluster* has agreed on, above the api versions that a single broker supports,
 * and this is how that agreement is changed.
 *
 * **Controller-only.** `KafkaApis` @ 2.8.2 answers the request on a broker that is not the active controller with
 * the top-level **41** (`NotController`), so {@see \Protocol\Kafka\Admin\AdminClient::updateFeatures()} sends it to
 * the controller and looks it up again once when it moved. A KRaft broker of Kafka 3.x forwards it to the
 * controller instead (KIP-590), so every node of the cluster answers it.
 *
 * A `max_version_level` **below 1** is not a version but the request to **delete** the finalized feature, and
 * every lowering - a deletion included - needs a downgrade to be allowed.
 *
 * **Version 1 (KIP-778, Kafka 3.3) made the two decisions of a downgrade explicit.** The `allow_downgrade`
 * boolean of the entry became the `upgrade_type` of {@see \Protocol\Kafka\Admin\UpgradeType} - an upgrade, a
 * *safe* downgrade that keeps every metadata record readable, or an *unsafe* one that does not - and the request
 * gained the top-level `validate_only` of `UpdateFeaturesOptions.validateOnly()`, with which the controller
 * answers what it *would* do without writing anything. {@see UpdateFeaturesRequestV0} is the frame below that,
 * with the boolean and without the dry run.
 *
 * **Version 2 (Kafka 4.0) is the frame of version 1 with another number in the header.** "Version 2 changes the
 * response to not return feature level results" stands above the `validVersions` of `UpdateFeaturesRequest.json`
 * @ 4.0.0: the version tells the controller that this client reads an answer without the per-feature `results`
 * ({@see UpdateFeaturesResponse}). {@see UpdateFeaturesRequestV1} is the same bytes one version lower.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 to v2)", "The upgrade type and the dry run
 *      of KIP-778 (v1)" and "The answer without results (v2, Kafka 4.0)"
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
    public const int VERSION = 2;

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
     * @param bool                            $validateOnly   Whether the controller only says what it would do
     *        (KIP-778, version 1)
     */
    public function __construct(
        array $featureUpdates,
        protected readonly int $timeoutMs = self::DEFAULT_TIMEOUT_MS,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Whether the controller validates the updates and writes none of them.
         *
         * @since Version 1 of protocol (Kafka 3.3, KIP-778)
         */
        protected readonly bool $validateOnly = false
    ) {
        $updateClass = static::featureUpdateClass();
        $updates     = [];
        foreach ($featureUpdates as $feature => $update) {
            $updates[(string) $feature] = new $updateClass(
                $update->feature,
                $update->maxVersionLevel,
                $update->getUpgradeType()
            );
        }
        $this->featureUpdates = $updates;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'timeoutMs'      => BinarySchema::TYPE_INT32,
            'featureUpdates' => ['feature' => static::featureUpdateClass()],
        ];
        if (static::VERSION >= 1) {
            $body['validateOnly'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }

    /**
     * Returns how long the controller may take over this request
     */
    public function getTimeoutMs(): int
    {
        return $this->timeoutMs;
    }

    /**
     * Returns whether the controller is asked to validate the updates without writing them (KIP-778, version 1)
     */
    public function isValidateOnly(): bool
    {
        return $this->validateOnly;
    }

    /**
     * Returns the updates of this request, indexed by the feature name
     *
     * @return array<string, FeatureUpdateKey>
     */
    public function getFeatureUpdates(): array
    {
        return $this->featureUpdates;
    }

    /**
     * Returns the class of an update entry for the version of the API that this class sends
     *
     * @return class-string<FeatureUpdateKey>
     */
    protected static function featureUpdateClass(): string
    {
        return static::VERSION >= 1 ? FeatureUpdateKey::class : FeatureUpdateKeyV0::class;
    }
}
