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
 * One topic of an Offsets (ListOffset) request, version 0
 *
 * <pre>
 *   OffsetsRequestTopic => TopicName [Partition Time MaxNumberOfOffsets]
 *     TopicName => string
 * </pre>
 *
 * Only the partition entries differ from version 1, so this class exists solely to lower the version constant that
 * {@see OffsetsRequestTopic::partitionClass()} follows.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v4), a.k.a. ListOffset"
 */
final class OffsetsRequestTopicV0 extends OffsetsRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
