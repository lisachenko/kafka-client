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
 * One node of the raft quorum and the endpoints it can be reached at (key 55, v2, Kafka 3.9, KIP-853)
 *
 * <pre>
 *   Node => NodeId [Listeners]
 *     NodeId    => INT32
 *     Listeners => COMPACT_ARRAY of {@see DescribeQuorumResponseListener}
 * </pre>
 *
 * `QuorumInfo.Node` of the Java admin client @ 3.9.2. The array is what KIP-853 needs to reconfigure a quorum at
 * all: the replica states of a partition name a voter by its id and its directory id alone, and this top-level
 * array is where the *address* of that id stands, so that a client of `AddRaftVoter` (80) or `RemoveRaftVoter`
 * (81) knows which endpoint it is talking about.
 *
 * It is a **top-level** array of the answer, next to the topics and not inside them: one entry per node of the
 * quorum, whatever partition it replicates. A node of a quorum that runs with `kraft.version` 0 - the static
 * `controller.quorum.voters` of the node of this line - is in it all the same: the array is written from the
 * voter set the leader holds, and the directory ids of its replica states are the zero uuid.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
class DescribeQuorumResponseNode implements BinarySchemaInterface
{
    /**
     * Id of the node this entry describes
     */
    public int $nodeId;

    /**
     * Endpoints of the node, indexed by the listener name
     *
     * @var array<string, DescribeQuorumResponseListener>
     */
    public array $listeners = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'nodeId'    => BinarySchema::TYPE_INT32,
            'listeners' => ['name' => DescribeQuorumResponseListener::class],
        ];
    }
}
