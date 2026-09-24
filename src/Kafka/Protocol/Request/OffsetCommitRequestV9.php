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
 * OffsetCommit request of version 9 (Kafka 3.6, KIP-848): the last version that names its topics
 *
 * The version 8 frame one number higher - "Version 9 is the first version that can be used with the new consumer
 * group protocol (KIP-848). The request is the same as version 8" in `OffsetCommitRequest.json` @ 3.6.2 - whose
 * answer may carry the 69 `GroupIdNotFound` of an unknown group and the 113 `StaleMemberEpoch` of a KIP-848 member.
 * Version 10 (Kafka 4.2, KIP-848) replaced the name of every topic entry with its id ({@see OffsetCommitRequest});
 * this version is what a client sends for a topic whose id it does not know, and what a node below Kafka 4.2 serves.
 * A 4.3.1 node answers a topic it does not have with the 3 `UNKNOWN_TOPIC_OR_PARTITION` here, where version 10
 * answers an id it does not know with the 100.
 *
 * @see docs/protocol/4.3.md, section "The member epoch of KIP-848 (v9)"
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetCommit (v10, KIP-848)"
 */
final class OffsetCommitRequestV9 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
