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
 * One topic of a Fetch request of the versions 0 to 4
 *
 * The topic entry never changed, only the partition entries it holds did, so this class only lowers the version
 * constant that {@see FetchRequestTopic::partitionClass()} follows.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v7)"
 */
final class FetchRequestTopicV0 extends FetchRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
