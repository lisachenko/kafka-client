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
 * One change to one finalized feature of an UpdateFeatures request (key 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   FeatureUpdateKey => Feature MaxVersionLevel AllowDowngrade
 *     Feature         => COMPACT_STRING
 *     MaxVersionLevel => INT16
 *     AllowDowngrade  => BOOLEAN
 * </pre>
 *
 * A `MaxVersionLevel` of **less than 1** is not a version at all but the request to **delete** the finalized
 * feature, and a deletion - like any lowering of the level - needs `AllowDowngrade` set, or the controller refuses
 * it with 96 (`FeatureUpdateFailed`).
 *
 * @see docs/protocol/2.8.md, section "UpdateFeatures API (key 57, v0)"
 */
class FeatureUpdateKey implements BinarySchemaInterface
{
    /**
     * Name of the feature to change
     */
    public string $feature;

    /**
     * New maximum finalized version level, less than 1 to delete the feature
     */
    public int $maxVersionLevel;

    /**
     * Whether the level may be lowered or the feature deleted
     */
    public bool $allowDowngrade;

    public function __construct(string $feature = '', int $maxVersionLevel = 1, bool $allowDowngrade = false)
    {
        $this->feature         = $feature;
        $this->maxVersionLevel = $maxVersionLevel;
        $this->allowDowngrade  = $allowDowngrade;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'feature'         => BinarySchema::TYPE_STRING,
            'maxVersionLevel' => BinarySchema::TYPE_INT16,
            'allowDowngrade'  => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}
