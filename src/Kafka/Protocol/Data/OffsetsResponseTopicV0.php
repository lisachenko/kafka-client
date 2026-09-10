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
 * One topic of an Offsets (ListOffset) response, version 0
 *
 * <pre>
 *   OffsetsResponseTopic => TopicName [Partition ErrorCode [Offset]]
 *     TopicName => string
 * </pre>
 *
 * Only the partition entries differ from version 1, so this class exists solely to lower the version constant that
 * {@see OffsetsResponseTopic::partitionClass()} follows.
 *
 * @see docs/protocol/1.1.md, section "Offsets API (key 2, v0, v1 and v2), a.k.a. ListOffset"
 */
final class OffsetsResponseTopicV0 extends OffsetsResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
