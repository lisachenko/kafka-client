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

namespace Protocol\Kafka\Admin;

/**
 * What {@see AdminClient::describeFeatures()} knows about the features of the cluster (KIP-584)
 *
 * `FeatureMetadata` of the Java admin client, and like it this is **not an api of its own**: the whole content of
 * it comes out of the tagged fields of an **ApiVersions v3** answer, which is why a client that speaks the
 * flexible version already knows it without another request.
 *
 * A ZooKeeper-backed Kafka 2.8.2 cluster finalizes no feature at all: `supportedFeatures` names what the broker
 * could agree to, `finalizedFeatures` is empty and `finalizedFeaturesEpoch` is `0`.
 *
 * @see docs/protocol/2.8.md, sections "ApiVersions API (key 18, v0 to v3)" and "UpdateFeatures API (key 57, v0)"
 */
final class FeatureMetadata
{
    /**
     * @param array<string, SupportedVersionRange> $supportedFeatures      What this broker supports, by feature name
     * @param array<string, FinalizedVersionRange> $finalizedFeatures      What the cluster agreed on, by name
     * @param int                                  $finalizedFeaturesEpoch Epoch of that agreement, -1 when unknown
     */
    public function __construct(
        public readonly array $supportedFeatures,
        public readonly array $finalizedFeatures,
        public readonly int $finalizedFeaturesEpoch
    ) {}
}
