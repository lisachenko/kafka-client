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
 * Implementation of the binary stream on top of a plain TCP socket.
 *
 * There is no SSL or SASL variant here on purpose: the 0.8 line has no security protocols at all, the broker only
 * ever speaks plaintext.
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
     * Flag that determines if the connection was established
     */
    protected bool $isConnected = false;

    /**
     * Socket stream constructor
     *
     * @param string               $tcpAddress        Tcp address for connection
     * @param array<string, mixed> $configuration     Configuration options
     * @param float|int|null       $connectionTimeout Timeout for the connection, in seconds
     */
    public function __construct(string $tcpAddress, protected array $configuration = [], $connectionTimeout = null)
    {
        $tcpInfo = parse_url($tcpAddress);
        if ($tcpInfo === false || !isset($tcpInfo['host'])) {
            throw new NetworkException(['error' => "Malformed tcp address: {$tcpAddress}"]);
        }
        $this->host = $tcpInfo['host'];
        $this->port = (int) ($tcpInfo['port'] ?? 9092);
        // ini_get() returns a string, whereas stream_socket_client() declares a float parameter
        $this->timeout = (float) ($connectionTimeout ?? ini_get('default_socket_timeout'));
    }

    public function write(string $format, ...$arguments): void
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $packedData = pack($format, ...$arguments);
        $totalBytes = strlen($packedData);

        $isReconnected = false;
        for ($written = 0; $written < $totalBytes;) {
            $result = @fwrite($this->streamSocket, substr($packedData, $written));
            if ($result === false || $result === 0) {
                // Nothing has been sent yet, so a dropped connection can still be retried transparently
                if (!$isReconnected && $written === 0 && !$this->isConnected()) {
                    $this->connect();
                    $isReconnected = true;
                    continue;
                }

                throw new NetworkException(['error' => 'Can not write to the stream', 'written' => $written]);
            }
            $written += $result;
        }
    }

    public function read(string $format): array
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $packetSize = self::packetSize($format);
        $arguments  = unpack($format, $this->readExactly($packetSize));
        if ($arguments === false) {
            throw new \InvalidArgumentException("Can not unpack the data with the format: {$format}");
        }

        return $arguments;
    }

    /**
     * Automatic resource clean up
     */
    final public function __destruct()
    {
        $this->disconnect();
    }

    public function isConnected(): bool
    {
        return is_resource($this->streamSocket) && stream_socket_get_name($this->streamSocket, true) !== false;
    }

    public function isEmpty(): bool
    {
        return !is_resource($this->streamSocket) || feof($this->streamSocket);
    }

    /**
     * Returns the underlying socket resource, opening the connection first if it is not established yet.
     *
     * The client waits for the answers of several brokers at once with `stream_select()`, which needs the socket
     * resources themselves; every other operation goes through the methods of this class.
     *
     * @return resource
     */
    public function getStreamSocket()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->streamSocket;
    }

    /**
     * @return array<string, mixed>
     */
    public function __debugInfo(): array
    {
        return [
            'host' => $this->host,
            'port' => $this->port,
        ];
    }

    /**
     * Reads exactly the requested amount of bytes, looping until they have all arrived.
     *
     * A single fread() on a socket returns whatever is available at that moment, which is regularly less than a
     * whole protocol frame.
     */
    protected function readExactly(int $length): string
    {
        if ($length <= 0) {
            return '';
        }

        $buffer        = '';
        $isReconnected = false;
        while (($received = strlen($buffer)) < $length) {
            $chunk = @fread($this->streamSocket, $length - $received);
            if ($chunk === false || $chunk === '') {
                $metadata = is_resource($this->streamSocket) ? stream_get_meta_data($this->streamSocket) : [];
                if (!empty($metadata['timed_out'])) {
                    throw new NetworkException([
                        'error'    => 'Timeout while reading from the stream',
                        'expected' => $length,
                        'received' => $received,
                    ]);
                }
                // Only a connection that dropped before the first byte of a frame can be retried transparently,
                // reconnecting in the middle of a frame would resume the parser at an arbitrary offset
                if (!$isReconnected && $received === 0 && !$this->isConnected()) {
                    $this->connect();
                    $isReconnected = true;
                    continue;
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
     * Performs connection to the specified socket address.
     *
     * A stream connects itself lazily on the first read or write; this method is public so that the connection
     * layer can establish a connection up front, before it hands the socket to `stream_select()`.
     */
    public function connect(): void
    {
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

        // The connection timeout only covers the handshake, the read/write timeout is driven by request.timeout.ms
        $requestTimeoutMs = (int) ($this->configuration[ClientConfig::REQUEST_TIMEOUT_MS] ?? ($this->timeout * 1000));
        stream_set_timeout($streamSocket, intdiv($requestTimeoutMs, 1000), ($requestTimeoutMs % 1000) * 1000);

        $this->streamSocket = $streamSocket;
        $this->isConnected  = true;
    }

    /**
     * Performs the disconnect operation.
     *
     * The stream stays usable afterwards: the next read or write opens a fresh connection to the same address.
     *
     * @see \Protocol\Kafka\Network\ConnectionFactory::close()
     */
    public function disconnect(): void
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
