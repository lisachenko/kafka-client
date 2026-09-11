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
 * lowering - a deletion included - needs `allowDowngrade`.
 *
 * @see docs/protocol/2.8.md, section "UpdateFeatures API (key 57, v0)"
 */
final class FeatureUpdate
{
    public function __construct(
        public readonly string $feature,
        public readonly int $maxVersionLevel,
        public readonly bool $allowDowngrade = false
    ) {}

    /**
     * Asks for the finalized feature to be removed from the cluster altogether
     */
    public static function delete(string $feature): self
    {
        return new self($feature, 0, true);
    }

    /**
     * Returns this update in the shape the api puts on the wire
     */
    public function toData(): FeatureUpdateKey
    {
        return new FeatureUpdateKey($this->feature, $this->maxVersionLevel, $this->allowDowngrade);
    }
}
