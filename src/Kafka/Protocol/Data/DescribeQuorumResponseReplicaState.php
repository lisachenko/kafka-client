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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * What the leader of a raft quorum knows about one replica of it (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   ReplicaState => ReplicaId LogEndOffset LastFetchTimestamp LastCaughtUpTimestamp
 *     ReplicaId             => INT32
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
 * {@see DescribeQuorumResponseReplicaStateV0} is the entry of the version below, which has neither field.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
class DescribeQuorumResponseReplicaState implements BinarySchemaInterface
{
    /**
     * Version of the DescribeQuorum API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * The value of a timestamp the leader does not know, and the `default` of both fields in the specification
     */
    public const int UNKNOWN_TIMESTAMP = -1;

    /**
     * Id of the replica this state belongs to
     */
    public int $replicaId;

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
        $scheme = [
            'replicaId'    => BinarySchema::TYPE_INT32,
            'logEndOffset' => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 1) {
            $scheme['lastFetchTimestamp']    = BinarySchema::TYPE_INT64;
            $scheme['lastCaughtUpTimestamp'] = BinarySchema::TYPE_INT64;
        }

        return $scheme;
    }
}
