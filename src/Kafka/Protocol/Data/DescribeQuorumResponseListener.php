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
 * One endpoint of a node of the raft quorum (key 55, v2, Kafka 3.9, KIP-853)
 *
 * <pre>
 *   Listener => Name Host Port
 *     Name => COMPACT_STRING
 *     Host => COMPACT_STRING
 *     Port => UINT16
 * </pre>
 *
 * `RaftVoterEndpoint` of the Java admin client, reached through `QuorumInfo.Node.endpoints()` @ 3.9.2. The `Name`
 * is the *listener* name of the endpoint, not a host name - `CONTROLLER` on the node of this line - so a quorum
 * whose voters listen on two listeners answers two of these per node.
 *
 * **The `Port` is the one `uint16` of the client-facing protocol** ({@see BinarySchema::TYPE_UINT16}): a port is
 * 0 to 65535, and an int16 would answer everything above 32767 as a negative number. It is the only field of any
 * api of Kafka 3.9.2 that uses the type, and the engine of this repository gained it for this structure.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
class DescribeQuorumResponseListener implements BinarySchemaInterface
{
    /**
     * Name of the listener this endpoint belongs to, e.g. `CONTROLLER`
     */
    public string $name;

    /**
     * Host name of the endpoint, as the node advertises it inside the cluster
     */
    public string $host;

    /**
     * Port of the endpoint, 0 to 65535
     */
    public int $port;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name' => BinarySchema::TYPE_STRING,
            'host' => BinarySchema::TYPE_STRING,
            'port' => BinarySchema::TYPE_UINT16,
        ];
    }
}
