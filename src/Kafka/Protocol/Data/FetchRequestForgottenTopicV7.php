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
 * One topic of the `forgotten_topics_data` array of a Fetch request of the versions 7 to 12
 *
 * The entry that names the topic by its **name**: `FetchRequest.json` @ 3.1.2 declares that `Topic` as
 * `versions 7-12`, and version 13 (Kafka 3.1, KIP-516) replaced it with the `topic_id` of
 * {@see FetchRequestForgottenTopic}. Nothing else about the entry ever changed - the partitions are an `int32`
 * array in every version, compact from version 12 on.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v17)" and "Fetch sessions (v7, KIP-227)"
 */
final class FetchRequestForgottenTopicV7 extends FetchRequestForgottenTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
