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

namespace Protocol\Kafka\Tests\Fixture;

use RuntimeException;

/**
 * Sends hand-built request frames to a real broker to find out what it does with a frame the client cannot build.
 *
 * The protocol classes of this branch can not be used for that: the whole point of the probe is to send frames that
 * the branch does not implement - an api key it does not know, a version above the highest one it declares, or a
 * body that deliberately does not match its schema - and to see what the broker does with them. Everything here is
 * therefore raw `pack()` on a plain socket. What the broker *does* serve is no longer probed at all on this line:
 * a 0.10 broker answers that with ApiVersions (key 18).
 *
 * A broker has three answers to such a frame, and the probe tells them apart:
 *
 * * {@see self::ANSWERED} - a complete response frame came back.
 * * {@see self::CLOSED} - the broker closed the connection. On **Kafka 0.10** this is what an unknown api key, an
 *   unsupported version or a body that does not match the schema looks like: `SocketServer.processCompletedReceives`
 *   @ 0.10.2.2 catches the `InvalidRequestException`/`SchemaException` of the parser and closes the channel, and the
 *   broker log carries "Closing socket for ... because of error" with the reason.
 * * {@see self::SILENT} - nothing came back and the connection stayed open. This is the answer of a **0.9.0.1**
 *   broker to a frame it cannot parse: the network thread logs "Processor got uncaught exception", drops the
 *   request and goes on, so the client waits for its own timeout. A 0.10.2.2 broker never does this, and the status
 *   is kept both to tell a hung connection from a closed one and because the fixture is cascaded upwards unchanged.
 *
 * @see \Protocol\Kafka\Tests\Integration\ApiVersionProbeTest
 */
final class RawApiProbe
{
    public const string ANSWERED = 'answered';

    public const string SILENT = 'silent';

    public const string CLOSED = 'closed';

    /**
     * How long to wait for a response frame before calling the request unanswered, in seconds
     */
    private const float SILENCE_TIMEOUT = 2.0;

    /**
     * Open connection to the broker
     *
     * @var resource
     */
    private $socket;

    /**
     * @param string $address Broker address as `host:port`
     */
    public function __construct(private readonly string $address, float $connectTimeout = 5.0)
    {
        $socket = @stream_socket_client('tcp://' . $address, $errorNumber, $errorString, $connectTimeout);
        if ($socket === false) {
            throw new RuntimeException("Can not connect to {$address}: {$errorString} ({$errorNumber})");
        }
        stream_set_blocking($socket, false);

        $this->socket = $socket;
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Sends one request frame and returns what the broker did with it
     *
     * @param int    $apiKey        Api key of the request
     * @param int    $apiVersion    Api version of the request
     * @param string $body          Request body, everything after the header
     * @param int    $correlationId Correlation id to send and to expect back
     * @param bool   $withClientId  Whether the header carries a client id (ControlledShutdown v0 does not)
     *
     * @return array{status: string, correlationId: int|null, body: string}
     */
    public function send(
        int $apiKey,
        int $apiVersion,
        string $body,
        int $correlationId,
        bool $withClientId = true
    ): array {
        $header = pack('nnN', $apiKey, $apiVersion, $correlationId);
        if ($withClientId) {
            $header .= self::string('kafka-client-t1-probe');
        }
        $frame = $header . $body;
        fwrite($this->socket, pack('N', strlen($frame)) . $frame);

        return $this->receive();
    }

    /**
     * Encodes a protocol string: an int16 length followed by its bytes
     */
    public static function string(string $value): string
    {
        return pack('n', strlen($value)) . $value;
    }

    /**
     * Encodes a protocol byte array: an int32 length followed by its bytes, `null` as the length -1
     */
    public static function bytes(?string $value): string
    {
        return $value === null ? pack('N', -1) : pack('N', strlen($value)) . $value;
    }

    /**
     * Encodes an int32 that PHP would otherwise pack as an unsigned value
     */
    public static function int32(int $value): string
    {
        return pack('N', $value);
    }

    /**
     * Encodes an int64 in the big endian order of the protocol
     */
    public static function int64(int $value): string
    {
        return pack('J', $value);
    }

    /**
     * Closes the connection to the broker
     */
    public function close(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }
    }

    /**
     * Reads one complete response frame, or reports that none came
     *
     * @return array{status: string, correlationId: int|null, body: string}
     */
    private function receive(): array
    {
        $deadline = microtime(true) + self::SILENCE_TIMEOUT;
        $buffer   = '';

        while (microtime(true) < $deadline) {
            $chunk = fread($this->socket, 65536);
            if ($chunk !== false) {
                $buffer .= $chunk;
            }
            if (strlen($buffer) >= 4) {
                $size = (int) unpack('N', substr($buffer, 0, 4))[1];
                if (strlen($buffer) >= 4 + $size) {
                    $frame = substr($buffer, 4, $size);

                    return [
                        'status'        => self::ANSWERED,
                        'correlationId' => (int) unpack('N', substr($frame, 0, 4))[1],
                        'body'          => substr($frame, 4),
                    ];
                }
            }
            if (feof($this->socket)) {
                return ['status' => self::CLOSED, 'correlationId' => null, 'body' => $buffer];
            }
            usleep(20000);
        }

        return ['status' => self::SILENT, 'correlationId' => null, 'body' => $buffer];
    }
}
