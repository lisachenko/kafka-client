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
 * Produce response Topic DTO of the versions 2, 3 and 4
 *
 * <pre>
 *   TopicName [Partition ErrorCode Offset LogAppendTime]
 *     TopicName => string
 * </pre>
 *
 * The topic entry never changed, only the partition entries it holds did, so this class only lowers the version
 * constant that {@see ProduceResponseTopic::partitionClass()} follows: the versions 2, 3 and 4 answer with the
 * `LogAppendTime` of version 2 and without the `LogStartOffset` of version 5.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v5)"
 */
final class ProduceResponseTopicV2 extends ProduceResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
