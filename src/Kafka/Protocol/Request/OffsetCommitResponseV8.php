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
 * OffsetCommit answer of version 8 (Kafka 2.4, KIP-482): the first flexible answer of the api
 *
 * The bytes are the ones of {@see OffsetCommitResponse}, field for field - "the response is the same as version
 * 8" in `OffsetCommitResponse.json` @ 3.6.2 - because KIP-848 gave version 9 no field, only two more error codes
 * it may carry: the **69** `GroupIdNotFound` of a group the coordinator does not know, which this version answers
 * as the **22** `IllegalGeneration` instead, and the **113** `StaleMemberEpoch` of a member of a KIP-848 group,
 * which can never reach a version 8 answer because such a member may not send a version 8 request.
 *
 * @see docs/protocol/4.3.md, section "The member epoch of KIP-848 (v9)"
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 */
final class OffsetCommitResponseV8 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
