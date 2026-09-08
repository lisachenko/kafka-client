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

namespace Protocol\Kafka\Tests\Unit\IO;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\AbstractStream;
use Protocol\Kafka\IO\SocketStream;

/**
 * Tests the socket stream against a local TCP server
 */
#[CoversClass(SocketStream::class)]
#[CoversClass(AbstractStream::class)]
final class SocketStreamTest extends TestCase
{
    /**
     * Listening socket of the fake broker
     *
     * @var resource|null
     */
    private $server;

    /**
     * Accepted server-side connection
     *
     * @var resource|null
     */
    private $connection;

    private ?SocketStream $stream = null;

    protected function setUp(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errorNumber, $errorString);
        if ($server === false) {
            self::markTestSkipped("Can not listen on a local TCP port: {$errorString}");
        }
        $this->server = $server;
    }

    protected function tearDown(): void
    {
        $this->stream = null;
        if (is_resource($this->connection)) {
            fclose($this->connection);
        }
        if (is_resource($this->server)) {
            fclose($this->server);
        }
        $this->connection = null;
        $this->server     = null;
    }

    public function testMalformedAddressIsRejected(): void
    {
        $this->expectException(NetworkException::class);
        new SocketStream('not a tcp address');
    }

    public function testConnectionRefusedIsReportedAsNetworkException(): void
    {
        // Close the listening socket, so nothing accepts on that port any more
        $address = stream_socket_get_name($this->server, false);
        fclose($this->server);
        $this->server = null;

        $stream = new SocketStream("tcp://{$address}", [], 1.0);

        $this->expectException(NetworkException::class);
        $stream->readInt32();
    }

    public function testConnectionTimeoutIsAFloatAndDoesNotFatalOnPhp84(): void
    {
        // The default connection timeout comes from the default_socket_timeout ini setting, which ini_get() returns
        // as a string; stream_socket_client() declares a float parameter and would fail under strict_types.
        $stream = $this->connectStream();

        $this->writeToClient('kafka');
        self::assertSame('kafka', $stream->readRaw(5));
    }

    public function testReadRawKeepsAccumulatingPartiallyDeliveredData(): void
    {
        $stream = $this->connectStream([ClientConfig::REQUEST_TIMEOUT_MS => 200]);

        // Only half of the requested frame ever arrives: the read loop has to buffer it and then hit the timeout,
        // which proves that a single fread() returning less than the requested amount is not treated as a result.
        $this->writeToClient('abc');

        try {
            $stream->readRaw(6);
            self::fail('An incomplete frame is expected to time out');
        } catch (NetworkException $exception) {
            $context = $exception->getContext();
            self::assertSame('Timeout while reading from the stream', $context['error']);
            self::assertSame(6, $context['expected']);
            self::assertSame(3, $context['received']);
        }
    }

    public function testReadRawReturnsExactlyTheRequestedAmountOfBytes(): void
    {
        $stream = $this->connectStream();

        $this->writeToClient('abc');
        $this->writeToClient('defgh');

        self::assertSame('abcdef', $stream->readRaw(6));
        self::assertSame('gh', $stream->readRaw(2));
    }

    public function testEofIsReportedAsNetworkException(): void
    {
        $stream = $this->connectStream();

        $this->writeToClient('ab');
        fclose($this->connection);
        $this->connection = null;

        try {
            // Only 2 of the 4 bytes will ever arrive, the server side is gone
            $stream->readInt32();
            self::fail('A closed connection is expected to be reported as a network error');
        } catch (NetworkException $exception) {
            $context = $exception->getContext();
            self::assertSame('Unexpected end of stream', $context['error']);
            self::assertSame(4, $context['expected']);
            self::assertSame(2, $context['received']);
        }
    }

    public function testRequestTimeoutIsHonouredWhileReading(): void
    {
        $stream = $this->connectStream([ClientConfig::REQUEST_TIMEOUT_MS => 200]);

        $startedAt = microtime(true);
        try {
            $stream->readInt32();
            self::fail('A read from a silent server is expected to time out');
        } catch (NetworkException $exception) {
            $context = $exception->getContext();
            self::assertSame('Timeout while reading from the stream', $context['error']);
        }
        self::assertGreaterThanOrEqual(0.15, microtime(true) - $startedAt);
    }

    public function testWrittenBytesReachTheServerUnmodified(): void
    {
        $stream = $this->connectStream();

        $stream->writeInt16(3);
        $stream->writeInt16(0);
        $stream->writeInt32(1);
        $stream->writeString('test');

        $this->acceptConnection();
        self::assertSame('0003' . '0000' . '00000001' . '0004' . '74657374', $this->readFromServer(14));
    }

    public function testTypedPrimitivesAreReadFromTheSocket(): void
    {
        $stream = $this->connectStream();

        $this->writeToClient(hex2bin('ff' . 'fffe' . 'ffffffff' . 'ffffffffffffffff' . 'ffff' . 'ffffffff'));

        self::assertSame(-1, $stream->readInt8());
        self::assertSame(-2, $stream->readInt16());
        self::assertSame(-1, $stream->readInt32());
        self::assertSame(-1, $stream->readInt64());
        self::assertNull($stream->readString());
        self::assertNull($stream->readBytes());
    }

    /**
     * Creates a connected socket stream and accepts the connection on the server side
     *
     * @param array<string, mixed> $configuration
     */
    private function connectStream(array $configuration = []): SocketStream
    {
        $address      = stream_socket_get_name($this->server, false);
        $this->stream = new SocketStream("tcp://{$address}", $configuration, 1.0);
        // Writing nothing still establishes the connection, so the server side can accept it
        $this->stream->writeRaw('');
        $this->acceptConnection();

        return $this->stream;
    }

    private function acceptConnection(): void
    {
        if ($this->connection !== null) {
            return;
        }
        $connection = stream_socket_accept($this->server, 5);
        if ($connection === false) {
            self::fail('The fake broker did not receive a connection');
        }
        $this->connection = $connection;
    }

    private function writeToClient(string $data): void
    {
        $this->acceptConnection();
        fwrite($this->connection, $data);
    }

    /**
     * Reads the given amount of bytes from the server side and returns them as a hex string
     */
    private function readFromServer(int $length): string
    {
        $buffer = '';
        while (strlen($buffer) < $length) {
            $chunk = fread($this->connection, $length - strlen($buffer));
            if ($chunk === false || $chunk === '') {
                break;
            }
            $buffer .= $chunk;
        }

        return bin2hex($buffer);
    }
}
