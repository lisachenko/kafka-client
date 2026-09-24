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

use Protocol\Kafka\Common\Uuid;

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
 * {@see self::$replicaDirectoryId} is the addition of KIP-853 (Kafka 3.9, the version 2): the directory the
 * replica keeps the metadata log in, which together with the id is the *key* of a voter, so that a node that comes
 * back with another disk is not mistaken for the one that went away. It is the zero uuid in an answer below the
 * version 2 and in an answer of a quorum whose `kraft.version` is still 0, i.e. one that is configured with the
 * static `controller.quorum.voters` of KIP-595 - which is what the node of this line is.
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The two timestamps of a
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
        public readonly ?int $lastCaughtUpTimestamp = null,
        /**
         * The 16 raw bytes of the directory this replica keeps the metadata log in; the zero uuid when the answer
         * does not name one, which is every answer below the version 2 and every quorum at `kraft.version` 0.
         */
        public readonly string $replicaDirectoryId = Uuid::ZERO
    ) {}

    /**
     * Returns the directory id in the readable `base64url` notation the Kafka tools print it in
     *
     * `Uuid.toString()` @ 3.9.2, i.e. `AAAAAAAAAAAAAAAAAAAAAA` for the zero uuid of a replica without one.
     */
    public function replicaDirectoryIdAsString(): string
    {
        return Uuid::toString($this->replicaDirectoryId);
    }
}
