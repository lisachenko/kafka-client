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
 * One endpoint of a node of the metadata quorum (KIP-853, Kafka 3.9)
 *
 * `RaftVoterEndpoint` of the Java admin client @ 3.9.2, which {@see QuorumNode::$endpoints} holds. The name is the
 * **listener** name of the endpoint - `CONTROLLER` on the node of this line - and not a host name; the host and
 * the port are the address that listener is advertised at inside the cluster, which is not necessarily one a
 * client outside it can reach.
 *
 * @see docs/protocol/3.9.md, section "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
final class RaftVoterEndpoint
{
    /**
     * @param string $name Name of the listener, e.g. `CONTROLLER`
     * @param string $host Host the listener is advertised at
     * @param int    $port Port of the listener, 0 to 65535 (the one `uint16` of the protocol)
     */
    public function __construct(
        public readonly string $name,
        public readonly string $host,
        public readonly int $port
    ) {}

    /**
     * Returns the endpoint as `host:port`, the way a bootstrap server is written
     */
    public function address(): string
    {
        return $this->host . ':' . $this->port;
    }
}
