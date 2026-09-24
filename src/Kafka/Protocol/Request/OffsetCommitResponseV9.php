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
 * OffsetCommit answer of version 9 (Kafka 3.6, KIP-848): the last version that names its topics
 *
 * The version 8 answer - "the response is the same as version 8" in `OffsetCommitResponse.json` @ 3.6.2 - which
 * may carry the 69 `GroupIdNotFound` and the 113 `StaleMemberEpoch`. Version 10 (Kafka 4.2, KIP-848) names every
 * topic of the answer by its id ({@see OffsetCommitResponse}).
 *
 * @see docs/protocol/4.3.md, section "The member epoch of KIP-848 (v9)"
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 */
final class OffsetCommitResponseV9 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
