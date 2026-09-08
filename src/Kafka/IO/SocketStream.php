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
 * Implementation of the binary stream on top of a plain TCP socket
 */
class SocketStream extends AbstractStream
{
    /**
     * Internal socket
     *
     * @var resource|null
     */
    protected $streamSocket;

    /**
     * Host name
     */
    protected string $host;

    /**
     * Port number
     */
    protected int $port;

    /**
     * Timeout for the connection, in seconds
     */
    protected float $timeout;

    /**
     * Flag that determines if connection was established
     */
    protected bool $isConnected = false;

    /**
     * Socket stream constructor
     *
     * @param string             $tcpAddress        Tcp address for connection
     * @param array<string, mixed> $configuration   Configuration options
     * @param float|int|null     $connectionTimeout Timeout for connection, in seconds
     */
    public function __construct(string $tcpAddress, protected array $configuration = [], $connectionTimeout = null)
    {
        $tcpInfo = parse_url($tcpAddress);
        if ($tcpInfo === false || !isset($tcpInfo['host'])) {
            throw new NetworkException(['error' => "Malformed tcp address: {$tcpAddress}"]);
        }
        $this->host    = $tcpInfo['host'];
        $this->port    = (int) ($tcpInfo['port'] ?? 9092);
        $this->timeout = (float) ($connectionTimeout ?? ini_get('default_socket_timeout'));
    }

    public function writeRaw(string $data): void
    {
        if (!$this->isConnected) {
            $this->connect();
        }

        $totalBytes = strlen($data);
        for ($written = 0; $written < $totalBytes;) {
            $result = @fwrite($this->streamSocket, substr($data, $written));
            if ($result === false || $result === 0) {
                throw new NetworkException(['error' => 'Can not write to the stream', 'written' => $written]);
            }
            $written += $result;
        }
    }

    public function readRaw(int $length): string
    {
        if ($length < 0) {
            throw new \InvalidArgumentException("Length should not be negative, {$length} given");
        }
        if ($length === 0) {
            return '';
        }
        if (!$this->isConnected) {
            $this->connect();
        }

        $buffer = '';
        while (($received = strlen($buffer)) < $length) {
            $chunk = @fread($this->streamSocket, $length - $received);
            if ($chunk === false || $chunk === '') {
                $metadata = stream_get_meta_data($this->streamSocket);
                if (!empty($metadata['timed_out'])) {
                    throw new NetworkException([
                        'error'    => 'Timeout while reading from the stream',
                        'expected' => $length,
                        'received' => $received,
                    ]);
                }

                throw new NetworkException([
                    'error'    => 'Unexpected end of stream',
                    'expected' => $length,
                    'received' => $received,
                ]);
            }
            $buffer .= $chunk;
        }

        return $buffer;
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
    protected function connect(): void
    {
        if ($this->isConnected) {
            return;
        }

        $socketFlags = STREAM_CLIENT_CONNECT;
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
        if ($streamSocket === false) {
            throw new NetworkException(['errorNumber' => $errorNumber, 'errorString' => $errorString]);
        }

        $sendBuffer    = $this->configuration[ClientConfig::SEND_BUFFER_BYTES] ?? null;
        $receiveBuffer = $this->configuration[ClientConfig::RECEIVE_BUFFER_BYTES] ?? null;
        if ($sendBuffer !== null) {
            stream_set_write_buffer($streamSocket, (int) $sendBuffer);
        }
        if ($receiveBuffer !== null) {
            stream_set_read_buffer($streamSocket, (int) $receiveBuffer);
        }

        // The read/write timeout is driven by request.timeout.ms, the connection timeout only covers the handshake
        $requestTimeoutMs = (int) ($this->configuration[ClientConfig::REQUEST_TIMEOUT_MS] ?? ($this->timeout * 1000));
        stream_set_timeout($streamSocket, intdiv($requestTimeoutMs, 1000), ($requestTimeoutMs % 1000) * 1000);

        $this->streamSocket = $streamSocket;
        $this->isConnected  = true;
    }

    /**
     * Performs the disconnect operation
     */
    protected function disconnect(): void
    {
        if (is_resource($this->streamSocket)
            && empty($this->configuration[ClientConfig::STREAM_PERSISTENT_CONNECTION])
        ) {
            fclose($this->streamSocket);
        }
        $this->streamSocket = null;
        $this->isConnected  = false;
    }
}
