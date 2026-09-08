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
 *   Broker => NodeId Host Port
 *     NodeId => int32
 *     Host   => string
 *     Port   => int32
 * </pre>
 *
 * The `Rack` of a broker only exists from version 1 of the Metadata API (Kafka 0.10.0) onwards.
 *
 * @see docs/protocol/0.8.2.md, section "Metadata API (key 3, v0)"
 */
class Node implements BinarySchemaInterface
{
    use RestorableTrait;

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
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'nodeId' => BinarySchema::TYPE_INT32,
            'host'   => BinarySchema::TYPE_STRING,
            'port'   => BinarySchema::TYPE_INT32,
        ];
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
