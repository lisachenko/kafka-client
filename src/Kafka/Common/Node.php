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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Information about a Kafka node
 *
 * <pre>
 *   Broker => NodeId Host Port Rack
 *     NodeId => int32
 *     Host   => string
 *     Port   => int32
 *     Rack   => nullable string
 * </pre>
 *
 * `METADATA_BROKER_V1` in `Protocol.java` @ 0.10.2.2: version 1 of the Metadata API (Kafka 0.10.0) appended the
 * `Rack` of the broker to the entry of `METADATA_BROKER_V0`, which {@see NodeV0} still describes. The rack is the
 * `broker.rack` of the broker configuration and is null for a broker that does not declare one - which is what the
 * broker of `docker-compose.yml` answers, like every broker of a cluster without rack awareness.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v7)"
 */
class Node implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * Version of the Metadata API that this entry is unpacked from
     */
    public const int VERSION = 1;

    /**
     * The broker id.
     */
    public int $nodeId = 0;

    /**
     * The hostname of the broker.
     */
    public string $host = '';

    /**
     * The port on which the broker accepts requests.
     */
    public int $port = 0;

    /**
     * The rack of the broker, null when it declares none or when the answer was a version 0 one.
     *
     * @since Version 1 of protocol
     */
    public ?string $rack = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'nodeId' => BinarySchema::TYPE_INT32,
            'host'   => BinarySchema::TYPE_STRING,
            'port'   => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 1) {
            $scheme['rack'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $scheme;
    }

    /**
     * Returns a connection to this node.
     *
     * The connection is kept open and handed out again for the next request to the same broker, see
     * {@see ConnectionFactory} for the lifetime of that cache.
     *
     * @param array<string, mixed> $configuration Client configuration
     *
     * @todo Move this method outside this class
     */
    public function getConnection(array $configuration): Stream
    {
        return ConnectionFactory::connect($this->host, $this->port, $configuration);
    }

    /**
     * Closes every open broker connection of this process
     */
    public static function closeConnections(): void
    {
        ConnectionFactory::closeAll();
    }
}
