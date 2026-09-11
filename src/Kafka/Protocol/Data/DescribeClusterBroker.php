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
 * One broker of a DescribeCluster answer (key 60, Kafka 2.8, KIP-700)
 *
 * <pre>
 *   DescribeClusterBroker => BrokerId Host Port Rack
 *     BrokerId => INT32
 *     Host     => COMPACT_STRING
 *     Port     => INT32
 *     Rack     => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * The same four fields a Metadata answer carries for a broker - KIP-700 did not change what is known about one,
 * it gave the cluster itself a request that does not have to name a single topic.
 *
 * @see docs/protocol/2.8.md, section "DescribeCluster API (key 60, v0)"
 */
class DescribeClusterBroker implements BinarySchemaInterface
{
    /**
     * Identifier of this broker
     */
    public int $brokerId;

    /**
     * Host name of the listener that answered
     */
    public string $host;

    /**
     * Port of that listener
     */
    public int $port;

    /**
     * Rack of the broker, null when it has none
     */
    public ?string $rack = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'brokerId' => BinarySchema::TYPE_INT32,
            'host'     => BinarySchema::TYPE_STRING,
            'port'     => BinarySchema::TYPE_INT32,
            'rack'     => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
