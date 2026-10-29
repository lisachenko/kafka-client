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
 * TxnOffsetCommit answer of version 5 (Kafka 4.0, KIP-890 part 2): the last version that names its topics
 *
 * "Version 5 is the same with version 3 (KIP-890)" (`TxnOffsetCommitResponse.json` @ 4.0.0). Version 6 (Kafka 4.4,
 * KIP-1319) names every topic of the answer by its id ({@see TxnOffsetCommitResponse}) and reports the 69
 * `GroupIdNotFound` and the 113 `StaleMemberEpoch` that this version folds into the 22 `IllegalGeneration`.
 *
 * @see docs/protocol/4.4.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 * @see docs/protocol/4.4.md, section "What Kafka 4.4 changed here (KIP-1319): the version 6"
 */
final class TxnOffsetCommitResponseV5 extends TxnOffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
