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
 * Topic entry of a Fetch request of the versions 9, 10 and 11
 *
 * The topic entry itself never changed; this class exists to pick {@see FetchRequestTopicPartitionV9} as its
 * partition entry, i.e. the entry without the `last_fetched_epoch` that version 12 (Kafka 2.7, KIP-595) added,
 * see {@see FetchRequestTopic}.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchRequestTopicV9 extends FetchRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
