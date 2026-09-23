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
 * One node of the metadata quorum and the endpoints it can be reached at (KIP-853, Kafka 3.9)
 *
 * `QuorumInfo.Node` of the Java admin client @ 3.9.2. It is called `QuorumNode` here because a nested class has no
 * PHP equivalent and {@see \Protocol\Kafka\Common\Node} - the broker of a Metadata answer - already carries the
 * plain name; a quorum node is not a broker, it is a voter or an observer of the raft log, and its endpoints are
 * the listeners of that role.
 *
 * The array is only in a **version 2** answer of DescribeQuorum: {@see QuorumInfo::$nodes} is empty after a
 * version 1 or a version 0 request, which have no such field at all.
 *
 * @see docs/protocol/3.9.md, section "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
final class QuorumNode
{
    /**
     * @param int                              $nodeId    Id of the node, the same id a replica state names
     * @param array<string, RaftVoterEndpoint> $endpoints Endpoints of the node, indexed by the listener name
     */
    public function __construct(
        public readonly int $nodeId,
        public readonly array $endpoints
    ) {}

    /**
     * Returns the endpoint of one listener of this node, null when the node does not advertise that listener
     */
    public function endpoint(string $listenerName): ?RaftVoterEndpoint
    {
        return $this->endpoints[$listenerName] ?? null;
    }
}
