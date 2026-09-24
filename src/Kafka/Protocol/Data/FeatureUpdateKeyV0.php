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

use Protocol\Kafka\Admin\UpgradeType;

/**
 * One change of an UpdateFeatures **v0** request: the entry with the `allow_downgrade` boolean of KIP-584
 *
 * <pre>
 *   FeatureUpdateKey => Feature MaxVersionLevel AllowDowngrade
 *     Feature         => COMPACT_STRING
 *     MaxVersionLevel => INT16
 *     AllowDowngrade  => BOOLEAN
 * </pre>
 *
 * Version 1 (KIP-778, Kafka 3.3) replaced that boolean with the `upgrade_type` of {@see FeatureUpdateKey}:
 * `AllowDowngrade` is declared `"versions": "0"` in `UpdateFeaturesRequest.json` @ 3.3.2 and travels in no frame
 * above this one. An entry of this class puts the intent of its {@see FeatureUpdateKey::$upgradeType} on the wire
 * as the boolean, so {@see UpgradeType::SafeDowngrade} and {@see UpgradeType::UnsafeDowngrade} are the same frame
 * here - which is exactly why KIP-778 needed the byte.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 to v2)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
final class FeatureUpdateKeyV0 extends FeatureUpdateKey
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
