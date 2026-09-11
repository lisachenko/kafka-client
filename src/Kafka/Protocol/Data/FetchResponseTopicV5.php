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

namespace Protocol\Kafka\Protocol\Data;

/**
 * Topic entry of a Fetch answer of the versions 5 to 10
 *
 * The topic entry itself never changed; this class exists to pick {@see FetchResponsePartitionV5} as its
 * partition entry, i.e. the entry without the `preferred_read_replica` that version 11 (Kafka 2.3, KIP-392)
 * added, see {@see FetchResponseTopic}.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchResponseTopicV5 extends FetchResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
