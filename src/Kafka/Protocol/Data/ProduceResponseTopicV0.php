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
 * Produce response Topic DTO of the versions 0 and 1
 *
 * <pre>
 *   TopicName [Partition ErrorCode Offset]
 *     TopicName => string
 * </pre>
 *
 * The topic entry itself did not change in version 2 of the API, only the partition entries it holds did, so this
 * class only lowers the version constant that {@see ProduceResponseTopic::partitionClass()} follows.
 *
 * @see docs/protocol/0.10.2.md, section "Produce API (key 0, v0, v1 and v2)"
 */
final class ProduceResponseTopicV0 extends ProduceResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
