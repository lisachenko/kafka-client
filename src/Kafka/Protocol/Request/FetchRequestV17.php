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

namespace Protocol\Kafka\Protocol\Request;

/**
 * Fetch request of version 17 (key 1)
 *
 * The version of the `replica_directory_id` of KIP-853 (Kafka 3.9), the last one without the tagged `high_watermark`
 * of a follower that version 18 (Kafka 4.1, KIP-1166) declares in every partition entry, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::$highWatermark}.
 *
 * A **consumer** writes the same bytes at both versions, because `Long.MAX_VALUE` - "the feature is not supported" -
 * is the default of that field and a tagged field whose value is its default is not written at all: the two frames
 * differ in the single byte of the api version of their header.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)", "The replica directory id of KIP-853 (v17)" and
 *      "The high watermark of a follower, KIP-1166 (v18)"
 */
final class FetchRequestV17 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 17;
}
