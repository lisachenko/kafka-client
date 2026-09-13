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
 * The range of a feature **one broker** supports, as the ApiVersions v3 answer reports it (KIP-584)
 *
 * `SupportedVersionRange` of the Java admin client.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
final class SupportedVersionRange
{
    public function __construct(public readonly int $minVersion, public readonly int $maxVersion) {}
}
