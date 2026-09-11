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
 * The level a cluster-wide **feature** is finalized at, tag 2 of an ApiVersions v3 answer (KIP-584, Kafka 2.7)
 *
 * <pre>
 *   FinalizedFeatureKey => Name MaxVersionLevel MinVersionLevel TAG_BUFFER
 *     Name            => COMPACT_STRING
 *     MaxVersionLevel => int16
 *     MinVersionLevel => int16
 * </pre>
 *
 * Note the order of the two bounds, which is the opposite of {@see ApiVersionsSupportedFeature}: the maximum comes
 * first, as `FinalizedFeatureKey` declares it in `ApiVersionsResponse.json` @ 2.8.2. The information is only
 * meaningful when the `finalizedFeaturesEpoch` of the same answer is `>= 0`, and it is written by the
 * `UpdateFeatures` api (key 57). The container of this line finalizes nothing, so the tagged field is absent from
 * its answer.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
class ApiVersionsFinalizedFeature implements BinarySchemaInterface
{
    /**
     * Name of the feature
     */
    public string $name;

    /**
     * Highest version level the cluster finalized for the feature
     */
    public int $maxVersionLevel;

    /**
     * Lowest version level the cluster finalized for the feature
     */
    public int $minVersionLevel;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'            => BinarySchema::TYPE_STRING,
            'maxVersionLevel' => BinarySchema::TYPE_INT16,
            'minVersionLevel' => BinarySchema::TYPE_INT16,
        ];
    }
}
