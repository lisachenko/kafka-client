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
 * @date   26.07.2016
 */

namespace Protocol\Kafka\IO;

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\NetworkException;

/**
 * Implementation of simple socket stream
 */
class SocketStream extends AbstractStream
{
    /**
     * Internal socket
     *
     * @var resource
     */
    protected $streamSocket;

    /**
     * Host name
     */
    protected string $host;

    /**
     * Port number
     *
     * @var integer
     */
    protected $port;

    /**
     * Timeout for connection
     *
     * @var integer
     */
    protected $timeout;

    /**
     * Flag that determines if connection was established
     *
     * @var boolean
     */
    protected $isConnected;

    /**
     * Socket stream constructor
     *
     * @param string  $tcpAddress        Tcp address for connection
     * @param array   $configuration     Configuration options
     * @param integer $connectionTimeout Timeout for connection
     */
    public function __construct($tcpAddress, protected array $configuration, $connectionTimeout = null)
    {
        $tcpInfo = parse_url($tcpAddress);
        if ($tcpInfo === false || !isset($tcpInfo['host'])) {
            throw new NetworkException(['error' => "Malformed tcp address: {$tcpAddress}"]);
        }
        $this->host          = $tcpInfo['host'];
        $this->port          = $tcpInfo['port'] ?? 9092;
        $this->timeout       = $connectionTimeout ?? ini_get("default_socket_timeout");
    }

    /**
     * Writes arguments to the stream
     *
     * @param string $format       Format for packing arguments
     * @param array  ...$arguments List of arguments for packing
     *
     * @see pack() manual for format
     *
     * @return void
     */
    public function write($format, ...$arguments): void
    {
        if (!$this->isConnected) {
            $this->connect();
        }

        $packedData = pack($format, ...$arguments);

        for ($written = 0; $written < strlen($packedData); $written += $result) {
            $result = @fwrite($this->streamSocket, substr($packedData, $written));
            if ($result === false || feof($this->streamSocket)) {
                throw new NetworkException(['error' => 'Can not write to the stream']);
            }
        }
    }

    /**
     * Reads information from the stream, advanced internal pointer
     *
     * @param string $format Format for unpacking arguments
     * @see unpack() manual for format
     *
     * @return array List of unpacked arguments
     */
    public function read($format): array|false
    {
        if (!$this->isConnected) {
            $this->connect();
        }

        $packetSize   = self::packetSize($format);
        $streamBuffer = '';

        for ($received = 0; $received < $packetSize; $received += strlen($result)) {
            $result = fread($this->streamSocket, $packetSize);
            if ($result === false || feof($this->streamSocket)) {
                throw new NetworkException(['error' => 'Can not read from the stream']);
            }
            $streamBuffer .= $result;
        }

        $arguments = unpack($format, $streamBuffer);

        return $arguments;
    }

    /**
     * Automatic resource clean up
     */
    final public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Performs connection to the specified socket address
     */
    protected function connect()
    {
        $socketFlags  = STREAM_CLIENT_CONNECT;
        if (!empty($this->configuration[ClientConfig::STREAM_ASYNC_CONNECT])) {
            $socketFlags |= STREAM_CLIENT_ASYNC_CONNECT;
        }
        if (!empty($this->configuration[ClientConfig::STREAM_PERSISTENT_CONNECTION])) {
            $socketFlags |= STREAM_CLIENT_PERSISTENT;
        }
        $streamSocket = @stream_socket_client(
            "tcp://{$this->host}:{$this->port}",
            $errorNumber,
            $errorString,
            $this->timeout,
            $socketFlags
        );
        if (!$streamSocket) {
            throw new NetworkException(['errorNumber' => $errorNumber, 'errorString' => $errorString]);
        }
        socket_set_option($streamSocket, SOL_SOCKET, SO_SNDBUF, $this->configuration[ClientConfig::SEND_BUFFER_BYTES]);
        socket_set_option($streamSocket, SOL_SOCKET, SO_RCVBUF, $this->configuration[ClientConfig::RECEIVE_BUFFER_BYTES]);

        $this->streamSocket = $streamSocket;
        $this->isConnected  = true;
    }

    /**
     * Performs the disconnect operation
     */
    protected function disconnect()
    {
        if (is_resource($this->streamSocket) && empty($this->configuration[ClientConfig::STREAM_PERSISTENT_CONNECTION])) {
            fclose($this->streamSocket);
        }
        $this->isConnected = false;
    }
}
