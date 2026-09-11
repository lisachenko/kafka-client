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
 * Topic entry of a Produce answer of the versions 5 to 7
 *
 * The topic entry itself never changed; this class exists to pick {@see ProduceResponsePartitionV5} as its
 * partition entry, i.e. the entry without the record errors that version 8 (Kafka 2.4, KIP-467) added, see
 * {@see ProduceResponseTopic}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v8)"
 */
final class ProduceResponseTopicV5 extends ProduceResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
