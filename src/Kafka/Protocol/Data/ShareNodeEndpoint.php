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
 * The endpoint of a leader named by a ShareFetch or ShareAcknowledge answer (keys 78 and 79, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   NodeEndpoint => node_id host port rack
 * </pre>
 *
 * *"Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER"* @ 4.1.0 - a
 * plain field at the end of both answers, empty on a one-node cluster.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 */
final class ShareNodeEndpoint implements BinarySchemaInterface
{
    /**
     * Id of the node
     */
    public int $nodeId = 0;

    /**
     * Host of the node
     */
    public string $host = '';

    /**
     * Port of the node
     */
    public int $port = 0;

    /**
     * Rack of the node, null when it has none
     */
    public ?string $rack = null;

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
