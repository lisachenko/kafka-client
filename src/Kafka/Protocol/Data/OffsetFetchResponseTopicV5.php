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
 * OffsetFetchResponseTopic DTO, the versions 5 to 9 of the OffsetFetch API: the entry that names its topic
 *
 * <pre>
 *   OffsetFetchResponseTopic => topic [partition_responses]
 *     topic               => STRING
 *     partition_responses => OffsetFetchResponsePartition   -- with the committed_leader_epoch of version 5
 * </pre>
 *
 * The entry of the versions 5 to 7 at the top level of the answer and of the versions 8 and 9 inside a group entry;
 * version 10 (Kafka 4.2, KIP-848) replaced its name with the `topic_id` of {@see OffsetFetchResponseTopic}.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 */
final class OffsetFetchResponseTopicV5 extends OffsetFetchResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
