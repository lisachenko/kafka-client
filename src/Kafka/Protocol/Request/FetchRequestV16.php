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
 * Fetch request of version 16 (key 1)
 *
 * The version of KIP-951, the last one whose partition entries have no tagged field of their own: version 17
 * (Kafka 3.9, KIP-853) declares the `replica_directory_id` inside them, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition::$replicaDirectoryId}.
 *
 * A **consumer** writes the same bytes at both versions, because the zero uuid of a client that names no
 * directory is the default of that field and a tagged field whose value is its default is not written at all: the
 * two frames differ in the single byte of the api version of their header. What the version 17 request buys is
 * therefore nothing for a consumer and the ability to name a log directory for a follower.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v17)", "The leader discovery of KIP-951 (v16)" and
 *      "The replica directory id of KIP-853 (v17)"
 */
final class FetchRequestV16 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 16;
}
