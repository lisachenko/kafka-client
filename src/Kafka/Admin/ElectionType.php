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
 * The kind of leader election an ElectLeaders request asks the controller for (`ElectionType` of the Java client)
 *
 * Kafka 2.2 added the api as **ElectPreferredLeaders** (KIP-183): it could elect the preferred replica and nothing
 * else, and its version 0 frame has no field for the kind of election at all. Kafka 2.4 renamed the api to
 * ElectLeaders and gave its version 1 a leading `election_type` byte (KIP-460), whose values are the two constants
 * below; a version 0 request is read by the broker as {@see self::PREFERRED}.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
final class ElectionType
{
    /**
     * Elect the **preferred** replica - the first one of the assignment - when it is in the ISR
     *
     * The election `kafka-preferred-replica-election.sh` and `auto.leader.rebalance.enable` perform, and the only
     * one a version 0 request can ask for.
     */
    public const int PREFERRED = 0;

    /**
     * Elect the first live replica even when no replica is in sync, accepting the data loss that comes with it
     *
     * `ElectionType.UNCLEAN` of the Java client, the election of KIP-460 and of `kafka-leader-election.sh
     * --election-type unclean`. It needs the **version 1** of the api, which Kafka 2.4 added and this line sends;
     * a request of {@see \Protocol\Kafka\Protocol\Request\ElectLeadersRequestV0} has no field to carry it and
     * always means the preferred election.
     *
     * The controller only acts on it for a partition whose leader is **gone**
     * (`KafkaController.processReplicaLeaderElection` @ 2.8.2: `currentLeader == LeaderAndIsr.NoLeader ||
     * !controllerContext.liveBrokerIds.contains(currentLeader)`); a partition that still has a live leader is
     * answered 84 `ElectionNotNeeded`, exactly as for a preferred election that is not needed.
     */
    public const int UNCLEAN = 1;

    private const array NAMES = [
        self::PREFERRED => 'PREFERRED',
        self::UNCLEAN   => 'UNCLEAN',
    ];

    private function __construct() {}

    /**
     * Returns whether the given value is one of the election types of the protocol
     */
    public static function isKnown(int $electionType): bool
    {
        return isset(self::NAMES[$electionType]);
    }

    /**
     * Returns the name of an election type, for an exception or a log line
     */
    public static function nameOf(int $electionType): string
    {
        return self::NAMES[$electionType] ?? "UNKNOWN({$electionType})";
    }
}
