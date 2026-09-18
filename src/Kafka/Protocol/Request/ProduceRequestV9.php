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
 * Produce request of version 9 (key 0)
 *
 * The first **flexible** version of the api (Kafka 2.8, KIP-482) and the last one below the leader discovery of
 * KIP-951: the request header v2, a compact `transactional_id` and topic name, compact arrays, a compact record
 * set and a tagged-field section behind the body, every topic entry and every partition entry.
 *
 * Version 10 (Kafka 3.7) sends this very body once more - `ProduceRequest.json` @ 3.7.2 declares no field of it
 * and its whole comment is "Version 10 is the same as version 9 (KIP-951)" - and what it buys is in the answer,
 * see {@see ProduceResponse::$nodeEndpoints}.
 *
 * @see docs/protocol/3.9.md, sections "Produce API (key 0, v0 to v10)" and "The leader discovery of KIP-951 (v10)"
 */
final class ProduceRequestV9 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
