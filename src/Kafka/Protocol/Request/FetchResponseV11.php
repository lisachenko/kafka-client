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
 * Fetch response of version 11 (key 1)
 *
 * The **plain** encoding of the api: `int16`-prefixed strings, `int32`-prefixed arrays and record sets, and no
 * tagged-field section anywhere. Version 11 (Kafka 2.3, KIP-392) is the one with the `rack_id` of the consumer
 * and the `preferred_read_replica` of the answer; version 12 (Kafka 2.7) is the first **flexible** version of
 * this api and adds the epoch validation of KIP-595 on top of it, see {@see FetchResponse}.
 *
 * The class inherits {@see FetchResponse::FLEXIBLE_VERSION} (12) and is therefore **not** flexible: the engine
 * asks `VERSION >= FLEXIBLE_VERSION`, and 11 is not.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v12)" and
 *      "Flexible versions in the engine (KIP-482)"
 */
final class FetchResponseV11 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 11;
}
