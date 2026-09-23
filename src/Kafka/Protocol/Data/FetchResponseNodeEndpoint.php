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
 * Where a leader named by a Fetch v16 answer can be reached, the `NodeEndpoint` of KIP-951
 *
 * One entry of the top-level tagged `node_endpoints` array (tag 0) that `FetchResponse.json` @ 3.7.2 added in
 * version 16 (Kafka 3.7): "Endpoints for all current-leaders enumerated in PartitionData, with errors
 * NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH". It is the missing half of the tagged `current_leader` that a
 * partition entry of this api has carried since version 12 ({@see FetchResponseCurrentLeader}), which names the
 * node id and the epoch but not the address they belong to.
 *
 * The array is written only for an answer that refused a partition with **6** `NOT_LEADER_OR_FOLLOWER` or **74**
 * `FENCED_LEADER_EPOCH` and knows the leader (`KafkaApis.handleFetchRequest` @ 3.9.2, the `versionId >= 16`
 * branch of `processResponseCallback`), and names each such node once, whatever the number of partitions that
 * point at it. The **host and the port are the ones of the listener the request arrived on**, so a consumer may
 * open the connection with them as they are and skip the Metadata round trip KIP-951 is about.
 *
 * @since Version 16 of the Fetch API (Kafka 3.7, KIP-951)
 *
 * @see docs/protocol/4.3.md, section "The leader discovery of KIP-951 (v16)"
 */
class FetchResponseNodeEndpoint implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO belongs to
     */
    public const int VERSION = 16;

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
