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
 * Fetch request of version 15 (key 1)
 *
 * The version of KIP-903: the body without the deprecated top-level `replica_id` and with the tagged
 * `replica_state` (tag 1) in its place, see {@see FetchRequest::$replicaState}.
 *
 * Version 16 (Kafka 3.7) sends this very frame once more - `FetchRequest.json` @ 3.7.2 declares no field of it
 * and its whole comment is "Version 16 is the same as version 15 (KIP-951)" - and what it buys is in the answer:
 * the tagged `node_endpoints` that names where the leader of a refused partition can be reached, see
 * {@see FetchResponse::$nodeEndpoints}.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v16)", "The replica state of KIP-903 (v15)" and
 *      "The leader discovery of KIP-951 (v16)"
 */
final class FetchRequestV15 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 15;
}
