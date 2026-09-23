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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One change to one finalized feature of an UpdateFeatures request (key 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   FeatureUpdateKey => Feature MaxVersionLevel AllowDowngrade | UpgradeType
 *     Feature         => COMPACT_STRING
 *     MaxVersionLevel => INT16
 *     AllowDowngrade  => BOOLEAN   -- version 0 only
 *     UpgradeType     => INT8      -- since version 1
 * </pre>
 *
 * A `MaxVersionLevel` of **less than 1** is not a version at all but the request to **delete** the finalized
 * feature, and a deletion - like any lowering of the level - needs a downgrade to be allowed.
 *
 * **Version 1 of the api (KIP-778, Kafka 3.3) replaced the boolean with the `upgrade_type`**, the int8 of
 * {@see UpgradeType}: "DEPRECATED in version 1 (see DowngradeType)" is what `UpdateFeaturesRequest.json` @ 3.3.2
 * writes above `AllowDowngrade`, whose `versions` is `0` there and nothing else. The boolean could only say that
 * *a* downgrade was allowed; the byte says whether the controller may lose the metadata of the higher level while
 * it does one ({@see UpgradeType::SafeDowngrade} against {@see UpgradeType::UnsafeDowngrade}).
 * {@see FeatureUpdateKeyV0} is the entry of the version below, which puts the same intent on the wire as the
 * boolean it had.
 *
 * The two fields are kept in step by the constructor, in the direction `FeatureUpdate.allowDowngrade()` @ 3.3.2
 * keeps them: the boolean is "anything but an upgrade".
 *
 * @see docs/protocol/3.9.md, sections "UpdateFeatures API (key 57, v0 and v1)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
class FeatureUpdateKey implements BinarySchemaInterface
{
    /**
     * Version of the UpdateFeatures API that this DTO encodes an entry of
     */
    public const int VERSION = 1;

    /**
     * Name of the feature to change
     */
    public string $feature;

    /**
     * New maximum finalized version level, less than 1 to delete the feature
     */
    public int $maxVersionLevel;

    /**
     * Whether the level may be lowered or the feature deleted, the field of the version 0 alone
     */
    public bool $allowDowngrade = false;

    /**
     * Which way the level may move: the code of a {@see UpgradeType}
     *
     * @since Version 1 of protocol (Kafka 3.3, KIP-778)
     */
    public int $upgradeType = UpgradeType::Upgrade->value;

    /**
     * The third argument is the upgrade type of the version 1 or, as a boolean, the `allow_downgrade` of the
     * version 0, which `FeatureUpdate(short, boolean)` @ 3.3.2 reads as {@see UpgradeType::SafeDowngrade}.
     */
    public function __construct(
        string $feature = '',
        int $maxVersionLevel = 1,
        bool|UpgradeType $upgrade = UpgradeType::Upgrade
    ) {
        $type = $upgrade instanceof UpgradeType
            ? $upgrade
            : ($upgrade ? UpgradeType::SafeDowngrade : UpgradeType::Upgrade);

        $this->feature         = $feature;
        $this->maxVersionLevel = $maxVersionLevel;
        $this->upgradeType     = $type->value;
        $this->allowDowngrade  = $type->allowsDowngrade();
    }

    /**
     * Returns the upgrade type this entry asks for, whichever of the two fields its version carries
     *
     * An entry of the version 0 has the boolean alone, and `FeatureUpdate(short, boolean)` @ 3.3.2 reads a set one
     * as {@see UpgradeType::SafeDowngrade}; an entry of the version 1 has the code, and a code the api does not
     * define is {@see UpgradeType::Unknown}.
     */
    public function getUpgradeType(): UpgradeType
    {
        if (static::VERSION < 1) {
            return $this->allowDowngrade ? UpgradeType::SafeDowngrade : UpgradeType::Upgrade;
        }

        return UpgradeType::fromCode($this->upgradeType);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'feature'         => BinarySchema::TYPE_STRING,
            'maxVersionLevel' => BinarySchema::TYPE_INT16,
        ];
        if (static::VERSION >= 1) {
            $scheme['upgradeType'] = BinarySchema::TYPE_INT8;
        } else {
            $scheme['allowDowngrade'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $scheme;
    }
}
