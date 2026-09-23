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
 * Fetch request of version 12 (key 1)
 *
 * The last version that names its topics by **name**. Version 12 (Kafka 2.7) is the first flexible version of the
 * api (KIP-482) and the one that added the `last_fetched_epoch` of KIP-595 and the tagged `cluster_id`; version 13
 * (Kafka 3.1, KIP-516) keeps that frame and only replaces the topic name of every topic entry and of every
 * `forgotten_topics_data` entry with the 16 raw bytes of the topic id, see {@see FetchRequest}.
 *
 * It is therefore the version a client falls back to while it does not know the id of a topic - and the version
 * that a **session** started with names: mixing the two in one session is the **106** `FetchSessionTopicIdError`
 * that Kafka 3.1 added.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v17)" and "The topic ids of the fetch path
 *      (v13, KIP-516)"
 */
final class FetchRequestV12 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
