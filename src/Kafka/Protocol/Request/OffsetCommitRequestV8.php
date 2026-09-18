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
 * OffsetCommit request of version 8 (Kafka 2.4, KIP-482): the first flexible frame of the api
 *
 * Version 9 (Kafka 3.6, KIP-848) sends the very same bytes one api version higher - "Version 9 is the first
 * version that can be used with the new consumer group protocol (KIP-848). The request is the same as version 8"
 * in `OffsetCommitRequest.json` @ 3.6.2 - and what the higher number buys is the **answer**: a commit that names
 * a group the coordinator does not know is refused with the 69 `GroupIdNotFound` from version 9 on, where this
 * version is answered the 22 `IllegalGeneration` of the backward compatibility, and a member of a KIP-848 group
 * may be told that its member epoch is stale with the 113. A member of such a group is not allowed to send this
 * version at all: `ConsumerGroup::validateOffsetCommit` @ 3.9.2 answers it 35 `UnsupportedVersion`.
 *
 * @see docs/protocol/3.9.md, section "The member epoch of KIP-848 (v9)"
 * @see docs/protocol/3.9.md, section "OffsetCommit API (key 8, v0 to v9)"
 */
final class OffsetCommitRequestV8 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
