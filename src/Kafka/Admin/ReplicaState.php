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

namespace Protocol\Kafka\Admin;

/**
 * One replica of the metadata quorum, as {@see AdminClient::describeMetadataQuorum()} reports it
 *
 * `QuorumInfo.ReplicaState` of the Java admin client. Everything here is what the **leader** of the quorum knows
 * about that replica, not what the replica itself would say.
 *
 * The two timestamps are milliseconds of the leader's wall clock and were added by KIP-836 (Kafka 3.3, the
 * version 1 of the api). A value the leader does not have is **null** here, exactly as it is an empty
 * `OptionalLong` in the Java client, and that is what the -1 of the wire and what a version 0 answer - which
 * carries neither field at all - both become.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
final class ReplicaState
{
    public function __construct(
        /**
         * Id of the node this state belongs to.
         */
        public readonly int $replicaId,
        /**
         * Last log end offset of this replica that the leader knows of.
         */
        public readonly int $logEndOffset,
        /**
         * When the leader last answered a fetch of this replica, null when it does not know it - which is what a
         * follower it has not heard from yet, and every replica of a version 0 answer, carries.
         */
        public readonly ?int $lastFetchTimestamp = null,
        /**
         * Append time of the offset this replica last fetched, null when the leader does not know it; the leader's
         * own entry carries the time it answered in both fields.
         */
        public readonly ?int $lastCaughtUpTimestamp = null
    ) {}
}
