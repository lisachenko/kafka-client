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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The range of a **feature** the broker supports, tag 0 of an ApiVersions v3 answer (KIP-584, Kafka 2.7)
 *
 * <pre>
 *   SupportedFeatureKey => Name MinVersion MaxVersion TAG_BUFFER
 *     Name       => COMPACT_STRING
 *     MinVersion => int16
 *     MaxVersion => int16
 * </pre>
 *
 * A "feature" of KIP-584 is a cluster-wide capability with a version range of its own, meant to replace
 * `inter.broker.protocol.version` in a KRaft cluster: the broker reports what it *can* do here, and what the cluster
 * has *agreed* on in {@see ApiVersionsFinalizedFeature}. The structure is called `SupportedFeatureKey` in
 * `ApiVersionsResponse.json` @ 2.8.2 and travels as a **tagged field**, which is why a ZooKeeper-backed 2.8.2
 * broker - which supports no feature at all - simply leaves it out of its answer instead of sending an empty array.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
class ApiVersionsSupportedFeature implements BinarySchemaInterface
{
    /**
     * Name of the feature
     */
    public string $name;

    /**
     * Lowest version of the feature the broker supports
     */
    public int $minVersion;

    /**
     * Highest version of the feature the broker supports
     */
    public int $maxVersion;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'       => BinarySchema::TYPE_STRING,
            'minVersion' => BinarySchema::TYPE_INT16,
            'maxVersion' => BinarySchema::TYPE_INT16,
        ];
    }
}
