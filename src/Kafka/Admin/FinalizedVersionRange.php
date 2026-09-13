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
 * The range of a feature the **cluster** has agreed on, as the ApiVersions v3 answer reports it (KIP-584)
 *
 * `FinalizedVersionRange` of the Java admin client. The finalized range is what {@see AdminClient::updateFeatures()}
 * changes, and it is always within the supported range of every broker of the cluster.
 *
 * @see docs/protocol/2.8.md, section "UpdateFeatures API (key 57, v0)"
 */
final class FinalizedVersionRange
{
    public function __construct(public readonly int $minVersionLevel, public readonly int $maxVersionLevel) {}
}
