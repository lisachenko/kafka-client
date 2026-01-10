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
use Protocol\Kafka\IO\SocketStream;

/**
 * Information about a ApiKeys node
 */
class Node
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

    /**
     * Unpacks the DTO from the binary buffer
     *
     * @param Stream $stream Binary buffer
     *
     * @return static
     */
    public static function unpack(Stream $stream): static
    {
        $brokerMetadata = new static();
        [$brokerMetadata->nodeId, $hostLength] = array_values($stream->read('NnodeId/nhostLength'));
        [$brokerMetadata->host, $brokerMetadata->port] = array_values($stream->read("a{$hostLength}host/Nport"));

        $brokerMetadata->rack = $stream->readString();

        return $brokerMetadata;
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
