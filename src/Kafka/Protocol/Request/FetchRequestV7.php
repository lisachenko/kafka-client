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
 * Fetch API (key 1), version 7
 *
 * <pre>
 *   FetchRequest (Version: 7) => ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel SessionId Epoch
 *                                [TopicName [Partition FetchOffset LogStartOffset MaxBytes]]
 *                                [TopicName [Partition]]
 * </pre>
 *
 * The body of version 7 (Kafka 1.1, KIP-227) is the body of version 8, byte for byte: `FetchRequest.json` @ 2.8.2
 * has no field of version 8 and says "Version 8 is the same as version 7". This is the highest version a **Kafka
 * 1.1.1** broker serves and the version the 1.x line of this client sent; a 2.8.2 broker still serves it, and
 * throttles it exactly as it throttles version 8 - by answering first and muting the channel - so a client that
 * sends this version only fails to announce that it understands KIP-219, see {@see FetchRequest}.
 *
 * The incremental fetch sessions of KIP-227 work exactly as they do at version 8: {@see FetchMetadata} in front of
 * the topics array and `forgotten_topics_data` behind it.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v12)" and "Fetch sessions (v7, KIP-227)"
 */
final class FetchRequestV7 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
