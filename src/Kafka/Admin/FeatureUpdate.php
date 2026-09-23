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

use Protocol\Kafka\Protocol\Data\FeatureUpdateKey;

/**
 * One change {@see AdminClient::updateFeatures()} asks the controller for
 *
 * `FeatureUpdate` of the Java admin client. A `maxVersionLevel` below 1 deletes the finalized feature, and every
 * lowering - a deletion included - needs a downgrade to be allowed.
 *
 * **Kafka 3.3 (KIP-778) split "a downgrade is allowed" in two**: `FeatureUpdate(short, UpgradeType)` @ 3.3.2
 * replaced the boolean of `FeatureUpdate(short, boolean)` with an {@see UpgradeType}, because a downgrade of a
 * feature can cost the cluster the metadata that the higher level wrote and the caller has to say whether it
 * accepts that. The boolean constructor of the Java client is deprecated and reads a set flag as
 * {@see UpgradeType::SafeDowngrade}, which is what this class does with one as well: a caller of the two-argument
 * form of the lines below sends exactly what it always sent.
 *
 * @see docs/protocol/3.9.md, sections "UpdateFeatures API (key 57, v0 and v1)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
final class FeatureUpdate
{
    /**
     * Which way the controller may move the level of this feature (KIP-778, the version 1 of the api)
     */
    public readonly UpgradeType $upgradeType;

    /**
     * Whether the level may be lowered, the boolean the version 0 of the api carries
     *
     * `FeatureUpdate.allowDowngrade()` @ 3.3.2, i.e. "the upgrade type is not an upgrade".
     */
    public readonly bool $allowDowngrade;

    /**
     * The third argument is the upgrade type of KIP-778 or, as a boolean, the `allow_downgrade` of KIP-584
     */
    public function __construct(
        public readonly string $feature,
        public readonly int $maxVersionLevel,
        bool|UpgradeType $upgrade = UpgradeType::Upgrade
    ) {
        $this->upgradeType    = $upgrade instanceof UpgradeType
            ? $upgrade
            : ($upgrade ? UpgradeType::SafeDowngrade : UpgradeType::Upgrade);
        $this->allowDowngrade = $this->upgradeType->allowsDowngrade();
    }

    /**
     * Asks for the finalized feature to be removed from the cluster altogether
     *
     * The level 0 is the deletion, and it needs a downgrade: the Java constructor refuses the level 0 together
     * with {@see UpgradeType::Upgrade} in as many words. A safe downgrade is what the boolean of KIP-584 meant,
     * so this is the deletion of both versions of the api.
     */
    public static function delete(string $feature, UpgradeType $upgrade = UpgradeType::SafeDowngrade): self
    {
        return new self($feature, 0, $upgrade);
    }

    /**
     * Returns this update in the shape the api puts on the wire
     */
    public function toData(): FeatureUpdateKey
    {
        return new FeatureUpdateKey($this->feature, $this->maxVersionLevel, $this->upgradeType);
    }
}
