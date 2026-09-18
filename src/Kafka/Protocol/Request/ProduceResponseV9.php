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
 * Produce response of version 9 (key 0)
 *
 * The first **flexible** answer of the api (Kafka 2.8, KIP-482) and the last one without the leader discovery of
 * KIP-951: the response header v1, compact strings and arrays, a tagged-field section behind every structure -
 * and nothing in any of those sections, because version 10 (Kafka 3.7) is what first declares a tag for this api.
 *
 * Its partition entry is {@see \Protocol\Kafka\Protocol\Data\ProduceResponsePartitionV8}, the entry without the
 * tagged `current_leader`, and the body carries no `node_endpoints`, see {@see ProduceResponse}.
 *
 * @see docs/protocol/3.9.md, sections "Produce API (key 0, v0 to v10)" and "The leader discovery of KIP-951 (v10)"
 */
final class ProduceResponseV9 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
