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
 * Produce request of version 12 (key 0)
 *
 * The transaction protocol v2 of KIP-890 part 2 (Kafka 4.0) and the last version that names its topics by **name**:
 * the flexible frame of version 9 with the version 12 in its header. A transactional batch of it on a node that
 * finalizes `transaction.version` 2 enrols its partition itself, see {@see ProduceRequestV11}.
 *
 * Version 13 (Kafka 4.1, KIP-516) replaced the name of every topic entry with its `topic_id` -
 * `ProduceRequest.json` @ 4.1.0: "Version 13 replaces topic names with topic IDs (KIP-516). May return
 * UNKNOWN_TOPIC_ID error code" - see {@see ProduceRequest}; this class keeps the version for a caller that knows
 * the names of its topics and not their ids.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The transaction protocol v2 of KIP-890
 *      part 2 (v12)"
 */
final class ProduceRequestV12 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
