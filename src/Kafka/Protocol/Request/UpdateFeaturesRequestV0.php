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

use Protocol\Kafka\Protocol\Data\FeatureUpdateKeyV0;

/**
 * UpdateFeatures request of version 0 (Kafka 2.7, KIP-584): the frame with `allow_downgrade` and no dry run
 *
 * Version 1 (KIP-778, Kafka 3.3) replaced the `allow_downgrade` boolean of every entry with the `upgrade_type`
 * byte ({@see FeatureUpdateKeyV0} against {@see \Protocol\Kafka\Protocol\Data\FeatureUpdateKey}) and appended the
 * top-level `validate_only`. This is the request below it: a safe and an unsafe downgrade are the very same bytes
 * here, and the {@see UpdateFeaturesRequest::$validateOnly} of an instance of this class never reaches the wire,
 * so the controller of such a request always writes what it accepts.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 and v1)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
final class UpdateFeaturesRequestV0 extends UpdateFeaturesRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
