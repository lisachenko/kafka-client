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
 * One broker of a DescribeCluster answer of version 2 (key 60, Kafka 2.8, KIP-700)
 *
 * <pre>
 *   DescribeClusterBroker => BrokerId Host Port Rack IsFenced
 *     BrokerId => INT32
 *     Host     => COMPACT_STRING
 *     Port     => INT32
 *     Rack     => COMPACT_NULLABLE_STRING
 *     IsFenced => BOOLEAN                  -- since version 2
 * </pre>
 *
 * The same four fields a Metadata answer carries for a broker - KIP-700 did not change what is known about one,
 * it gave the cluster itself a request that does not have to name a single topic.
 *
 * **Version 2 (KIP-1073, Kafka 4.0) appended `is_fenced`**: "Version 2 adds IsFenced field to Brokers for KIP-1073
 * support" (`DescribeClusterResponse.json` @ 4.0.0). It is true only for a broker the request asked for with
 * `include_fenced_brokers`, because an answer without that flag lists no fenced broker at all.
 * {@see DescribeClusterBrokerV1} is the entry of the versions 0 and 1.
 *
 * @see docs/protocol/4.3.md, sections "DescribeCluster API (key 60, v0 to v2)" and "The fenced brokers of KIP-1073
 *      (v2)"
 */
class DescribeClusterBroker implements BinarySchemaInterface
{
    /**
     * Version of the DescribeCluster API that this DTO is unpacked from
     */
    public const int VERSION = 2;

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
     * Whether the controller has fenced this broker
     *
     * @since Version 2 of protocol (Kafka 4.0, KIP-1073)
     */
    public bool $isFenced = false;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'brokerId' => BinarySchema::TYPE_INT32,
            'host'     => BinarySchema::TYPE_STRING,
            'port'     => BinarySchema::TYPE_INT32,
            'rack'     => BinarySchema::TYPE_NULLABLE_STRING,
        ];
        if (static::VERSION >= 2) {
            $scheme['isFenced'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $scheme;
    }
}
