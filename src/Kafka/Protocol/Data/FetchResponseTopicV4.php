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
 * One topic of a Fetch response of the version 4
 *
 * The topic entry of a version 4 answer is the one of version 5, only its partition entries carry no
 * `LogStartOffset`, so this class only lowers the version constant that
 * {@see FetchResponseTopic::partitionClass()} follows.
 *
 * @see docs/protocol/1.1.md, section "Fetch API (key 1, v0 to v7)"
 */
final class FetchResponseTopicV4 extends FetchResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
