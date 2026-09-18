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
 * Which way a {@see FeatureUpdate} may move the finalized level of a feature (Kafka 3.3, KIP-778)
 *
 * `FeatureUpdate.UpgradeType` of the Java admin client, an enum of the same four values. The version 0 of
 * UpdateFeatures had one boolean for this - `allow_downgrade` - which could only say "a downgrade is allowed"
 * without saying *which* kind, and the version 1 of the api replaced it with an int8
 * (`UpdateFeaturesRequest.json` @ 3.3.2). A downgrade of a feature can lose the metadata the higher level wrote,
 * and KIP-778 made the caller say whether it accepts that loss:
 *
 * * {@see self::Upgrade} raises the level and nothing else, which is the default of the field;
 * * {@see self::SafeDowngrade} lowers it as far as the controller can without losing metadata;
 * * {@see self::UnsafeDowngrade} lowers it whatever it costs.
 *
 * {@see self::Unknown} is the zero of the enum, which the Java client answers for any other code and never sends.
 *
 * @see docs/protocol/3.9.md, section "The upgrade type and the dry run of KIP-778 (v1)"
 */
enum UpgradeType: int
{
    /**
     * Not one of the three codes of the api, the value `FeatureUpdate.UpgradeType.fromCode` answers for them
     */
    case Unknown = 0;

    /**
     * Raise the finalized level of the feature; a request that would lower it is refused
     */
    case Upgrade = 1;

    /**
     * Lower the finalized level as far as it can be lowered without losing metadata ("lossless")
     */
    case SafeDowngrade = 2;

    /**
     * Lower the finalized level even when metadata is lost by it ("lossy")
     */
    case UnsafeDowngrade = 3;

    /**
     * Returns the upgrade type of a code of the wire, {@see self::Unknown} for a code no version of the api defines
     *
     * `FeatureUpdate.UpgradeType.fromCode` @ 3.3.2, which maps everything but 1, 2 and 3 to `UNKNOWN`.
     */
    public static function fromCode(int $code): self
    {
        return self::tryFrom($code) ?? self::Unknown;
    }

    /**
     * Whether this type lets the controller lower the level of a feature
     *
     * The `allow_downgrade` of the version 0 request, which `FeatureUpdate.allowDowngrade()` @ 3.3.2 answers as
     * "anything but an upgrade": a version 0 frame carries that boolean in the place of the type itself.
     */
    public function allowsDowngrade(): bool
    {
        return $this !== self::Upgrade;
    }
}
