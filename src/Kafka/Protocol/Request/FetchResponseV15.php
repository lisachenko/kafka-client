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
 * Fetch response of version 15 (key 1)
 *
 * The last answer of this api **without** the top-level `node_endpoints` of KIP-951: the frame of version 13,
 * whose topics are named by id, whose partition entries carry the three tagged fields of KIP-595 and KIP-630 -
 * the `current_leader` among them - and whose body ends in an empty tagged-field section. `FetchResponse.json`
 * @ 3.5.2 declares no field of the versions 14 and 15 at all, so these bytes are the bytes of a version 13
 * answer, and {@see FetchResponseV14} and {@see FetchResponseV13} decode them as well.
 *
 * Version 16 (Kafka 3.7, KIP-951) puts the endpoints of the leaders a refused partition points at into that
 * empty section, see {@see FetchResponse::$nodeEndpoints}.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v17)" and "The leader discovery of KIP-951 (v16)"
 */
final class FetchResponseV15 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 15;
}
