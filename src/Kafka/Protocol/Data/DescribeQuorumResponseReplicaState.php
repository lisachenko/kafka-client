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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * What the leader of a raft quorum knows about one replica of it (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   ReplicaState => ReplicaId ReplicaDirectoryId LogEndOffset LastFetchTimestamp LastCaughtUpTimestamp
 *     ReplicaId             => INT32
 *     ReplicaDirectoryId    => UUID    -- since version 2
 *     LogEndOffset          => INT64
 *     LastFetchTimestamp    => INT64   -- since version 1
 *     LastCaughtUpTimestamp => INT64   -- since version 1
 * </pre>
 *
 * The same structure stands for a **voter** and for an **observer** of the quorum
 * ({@see DescribeQuorumResponsePartition}); what it holds is what the leader knows, so every value of it is the
 * leader's view and not the replica's own.
 *
 * **Version 1 (KIP-836, Kafka 3.3) appended the two timestamps** - "Version 1 adds LastFetchTimeStamp and
 * LastCaughtUpTimestamp in ReplicaState (KIP-836)" in `DescribeQuorumResponse.json` @ 3.3.2 - so that an operator
 * can tell a follower that is *behind* from one that is **gone**: without them a stale `log_end_offset` says
 * nothing about when it was last heard from. Both are milliseconds of the **leader's** wall clock, both are
 * `ignorable` with the default **-1**, and -1 is what an unknown value looks like - which
 * `QuorumInfo.ReplicaState` @ 3.3.2 turns into an empty `OptionalLong`:
 *
 * * `LastFetchTimestamp` is when the leader last answered a fetch of this replica;
 * * `LastCaughtUpTimestamp` is the append time of the offset the replica last fetched, i.e. how far back the data
 *   it holds reaches.
 *
 * **The `about` of the first field is wrong about the leader.** It says the value "is reported as -1 both for the
 * current leader or if it is unknown for a voter", but `LeaderState.describeReplicaState` @ 3.3.2 sets **both**
 * timestamps to `currentTimeMs` when the entry is the leader's own, and the 3.9.2 node answers exactly that: the
 * leader's row carries a real millisecond in both fields. The -1 belongs to a *follower* the leader has not heard
 * from yet.
 *
 * **Version 2 (KIP-853, Kafka 3.9) put a `ReplicaDirectoryId` in front of the offset**, and it is the one field
 * of this structure that is not appended: the uuid stands between the replica id and the log end offset, because
 * the pair `(id, directory id)` is what KIP-853 calls the *key* of a voter - a node that loses its disk and comes
 * back with a new directory is a different replica to the quorum, whatever its id says. A quorum that still runs
 * the static `controller.quorum.voters` of KIP-595, i.e. with the feature `kraft.version` at the level **0**, has
 * no directory ids to report and answers {@see Uuid::ZERO} for every replica - which is what the node of this line
 * does, although its `meta.properties` carries a real `directory.id`.
 *
 * {@see DescribeQuorumResponseReplicaStateV1} is the entry without that uuid, and
 * {@see DescribeQuorumResponseReplicaStateV0} the entry of the version below that, which has no timestamp either.
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
class DescribeQuorumResponseReplicaState implements BinarySchemaInterface
{
    /**
     * Version of the DescribeQuorum API that this DTO is unpacked from
     */
    public const int VERSION = 2;

    /**
     * The value of a timestamp the leader does not know, and the `default` of both fields in the specification
     */
    public const int UNKNOWN_TIMESTAMP = -1;

    /**
     * Id of the replica this state belongs to
     */
    public int $replicaId;

    /**
     * The 16 raw bytes of the directory this replica keeps the metadata log in, the zero uuid when it has none
     *
     * @since Version 2 of protocol (Kafka 3.9, KIP-853)
     */
    public string $replicaDirectoryId = Uuid::ZERO;

    /**
     * Last log end offset the leader knows of this replica, -1 when it is unknown
     */
    public int $logEndOffset;

    /**
     * Wall clock time of the leader when this replica last fetched from it, -1 when it does not know it
     *
     * @since Version 1 of protocol (Kafka 3.3, KIP-836)
     */
    public int $lastFetchTimestamp = self::UNKNOWN_TIMESTAMP;

    /**
     * Append time of the offset this replica last fetched, the current time in the leader's own entry
     *
     * @since Version 1 of protocol (Kafka 3.3, KIP-836)
     */
    public int $lastCaughtUpTimestamp = self::UNKNOWN_TIMESTAMP;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = ['replicaId' => BinarySchema::TYPE_INT32];
        if (static::VERSION >= 2) {
            $scheme['replicaDirectoryId'] = BinarySchema::TYPE_UUID;
        }
        $scheme['logEndOffset'] = BinarySchema::TYPE_INT64;
        if (static::VERSION >= 1) {
            $scheme['lastFetchTimestamp']    = BinarySchema::TYPE_INT64;
            $scheme['lastCaughtUpTimestamp'] = BinarySchema::TYPE_INT64;
        }

        return $scheme;
    }
}
