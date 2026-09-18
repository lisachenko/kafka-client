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
 * Where a leader named by a Produce v10 answer can be reached, the `NodeEndpoint` of KIP-951
 *
 * One entry of the top-level tagged `node_endpoints` array (tag 0) that `ProduceResponse.json` @ 3.7.2 added in
 * version 10 (Kafka 3.7): "Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors
 * NOT_LEADER_OR_FOLLOWER". The node id, the host and the port are the three fields a client needs to open a
 * connection, and the `rack` is the nullable one of the broker's `broker.rack`.
 *
 * The array is written **only** for an answer that names a leader in the tagged `current_leader` of one of its
 * partition entries ({@see ProduceResponseCurrentLeader}), and it names each such node once, whatever the number
 * of partitions that point at it: `KafkaApis.handleProduceRequest` @ 3.9.2 collects them in a
 * `mutable.HashMap[Int, Node]` keyed by the node id. The **host and the port are the ones of the listener the
 * request arrived on**, not of some canonical listener, so a client may use them as they are.
 *
 * @since Version 10 of the Produce API (Kafka 3.7, KIP-951)
 *
 * @see docs/protocol/3.9.md, section "The leader discovery of KIP-951 (v10)"
 */
class ProduceResponseNodeEndpoint implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO belongs to
     */
    public const int VERSION = 10;

    /**
     * Node id of the broker this entry describes, the key of the array
     */
    public int $nodeId = 0;

    /**
     * Host name of that broker, as the listener of this connection advertises it
     */
    public string $host = '';

    /**
     * Port of that broker on the same listener
     */
    public int $port = 0;

    /**
     * Rack of that broker, `null` when it has not been assigned to one - the default of the specification
     */
    public ?string $rack = null;

    public function __construct(int $nodeId = 0, string $host = '', int $port = 0, ?string $rack = null)
    {
        $this->nodeId = $nodeId;
        $this->host   = $host;
        $this->port   = $port;
        $this->rack   = $rack;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'nodeId' => BinarySchema::TYPE_INT32,
            'host'   => BinarySchema::TYPE_STRING,
            'port'   => BinarySchema::TYPE_INT32,
            'rack'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
