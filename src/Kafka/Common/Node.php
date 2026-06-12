<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Common;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Information about a Kafka node
 */
class Node implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * The broker id.
     *
     * @var integer
     */
    public $nodeId;

    /**
     * The hostname of the broker.
     *
     * @var string
     */
    public $host;

    /**
     * The port on which the broker accepts requests.
     *
     * @var integer
     */
    public $port;

    /**
     * The rack of the broker.
     *
     * @var string
     * @since Version 1 of protocol
     */
    public $rack;

    /**
     * Cached list of connections
     */
    private static array $nodeConnections = [];

    public static function getScheme(): array
    {
        return [
            'nodeId' => BinarySchema::TYPE_INT32,
            'host'   => BinarySchema::TYPE_STRING,
            'port'   => BinarySchema::TYPE_INT32,
            'rack'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }

    /**
     * Returns a connection to this node.
     *
     * @param array $configuration Client configuration
     *
     * @return Stream
     */
    public function getConnection(array $configuration)
    {
        if (!isset(self::$nodeConnections[$this->host][$this->port])) {
            $connection = new SocketStream("tcp://{$this->host}:{$this->port}", $configuration);

            self::$nodeConnections[$this->host][$this->port] = $connection;
        }

        return self::$nodeConnections[$this->host][$this->port];
    }
}
