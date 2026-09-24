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

/**
 * UpdateFeatures answer of version 1 (Kafka 3.3, KIP-778): the answer with a result for every feature
 *
 * The bytes of the version 0 - `UpdateFeaturesResponse.json` @ 3.3.2 declares every field `0+` - and the last
 * version whose answer carries the per-feature `results`: `UpdateFeaturesResponse.json` @ 4.0.0 declares them
 * `"versions": "0-1"`. A 4.x controller fills them only when it accepted the whole request, and then every entry
 * carries the code 0; a refusal is the top-level error of the answer, with no result at all.
 *
 * @see docs/protocol/4.3.md, sections "The upgrade type and the dry run of KIP-778 (v1)" and "The answer without
 *      results (v2, Kafka 4.0)"
 */
final class UpdateFeaturesResponseV1 extends UpdateFeaturesResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
