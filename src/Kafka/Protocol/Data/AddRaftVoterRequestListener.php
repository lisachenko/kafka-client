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
 * One endpoint of the voter an AddRaftVoter request adds (key 80, v0, Kafka 3.9, KIP-853)
 *
 * <pre>
 *   Listener => Name Host Port
 *     Name => COMPACT_STRING
 *     Host => COMPACT_STRING
 *     Port => UINT16
 * </pre>
 *
 * `AddRaftVoterRequestData.Listener` @ 4.0.0, which the Java admin client fills from the `RaftVoterEndpoint`s the
 * caller names - the same three fields as the listener of a DescribeQuorum v2 answer
 * ({@see DescribeQuorumResponseListener}), with the one `uint16` of the protocol for the port. The controller
 * needs the endpoint of **its own** listener among them (`CONTROLLER` on the node of this line): it is where it
 * sends the ApiVersions that asks the new voter which `kraft.version` it supports, and a request without it is
 * answered 42 (`InvalidRequest`) before anything else is looked at.
 *
 * @see docs/protocol/4.3.md, section "AddRaftVoter API (key 80, v0 and v1)"
 */
class AddRaftVoterRequestListener implements BinarySchemaInterface
{
    /**
     * Name of the listener this endpoint belongs to, e.g. `CONTROLLER`
     */
    public string $name;

    /**
     * Host name of the endpoint
     */
    public string $host;

    /**
     * Port of the endpoint, 0 to 65535
     */
    public int $port;

    /**
     * @param string $name Name of the listener, e.g. `CONTROLLER`
     * @param string $host Host name of the endpoint
     * @param int    $port Port of the endpoint, 0 to 65535
     */
    public function __construct(string $name = '', string $host = '', int $port = 0)
    {
        $this->name = $name;
        $this->host = $host;
        $this->port = $port;
    }

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
