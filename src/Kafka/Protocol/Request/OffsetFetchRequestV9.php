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
 * OffsetFetch request of version 9 (Kafka 3.7, KIP-848): the last version that names its topics
 *
 * The batch of version 8 with the member id and the member epoch of KIP-848 in every group entry, whose topic
 * entries carry the topic **name** - `OffsetFetchRequest.json` @ 4.2.0 gives the `Name` of a topic entry the
 * versions `8-9`. Version 10 (Kafka 4.2, KIP-848) named the topics by id instead ({@see OffsetFetchRequest}); this
 * version is what a client sends for a topic whose id it does not know, and what a node below Kafka 4.2 serves.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The member id and epoch of KIP-848 (v9)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetFetch (v10, KIP-848)"
 */
final class OffsetFetchRequestV9 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
