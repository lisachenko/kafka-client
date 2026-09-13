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
 * Fetch request Topic DTO of the versions 5 to 8
 *
 * The topic entry itself never changed; what the version constant selects is the shape of its partition entries,
 * see {@see FetchRequestTopic::partitionClass()}. The versions 5 to 8 send the entry of
 * {@see FetchRequestTopicPartitionV5}, which carries the `LogStartOffset` of KIP-107 and no leader epoch.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchRequestTopicV5 extends FetchRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
