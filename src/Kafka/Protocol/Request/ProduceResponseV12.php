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
 * Produce response of version 12 (key 0)
 *
 * The answer of the transaction protocol v2 of KIP-890 part 2 (Kafka 4.0): the version 10 frame, whose topic
 * entries name their topic by **name**. `ProduceResponse.json` @ 4.1.0 replaced that name with the `topic_id` in
 * version 13 (KIP-516), see {@see ProduceResponse}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The transaction protocol v2 of KIP-890
 *      part 2 (v12)"
 */
final class ProduceResponseV12 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
