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
 * the branch does not implement - an api key it does not know, a version above the highest one it declares, a
 * version whose classes another ticket of the line still has to write, or a body that deliberately does not match
 * its schema - and to see what the broker does with them. Everything here is therefore raw `pack()` on a plain
 * socket. What the broker *does* serve is no longer probed at all on this line: a 0.10 broker answers that with
 * ApiVersions (key 18).
 *
 * Kafka 2.4 (KIP-482) gave the protocol a second encoding, and the probe has to speak it because more than half of
 * the apis of a 2.8.2 broker are **flexible** at their maximum version: a compact string, byte array or array
 * announces its length as an unsigned varint of `length + 1` (`0` meaning `null`), every structure ends in a
 * tagged-field section, and the request header grows one as well ({@see self::HEADER_V2}). The helpers below build
 * those bytes itself and stays independent of the schema engine on purpose: the probe has to be able to send a frame
 * the engine refuses - a compact length that is one too large, a structure without its tagged-field section, a
 * version above the table - which is exactly what the engine of {@see \Protocol\Kafka\Protocol\BinarySchema}
 * cannot produce.
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
     * Request header **v0**: api key, api version and correlation id, without a client id
     *
     * The one frame of the protocol that uses it is ControlledShutdown v0 (`RequestHeader.json` @ 2.8.2 says so in
     * as many words), and it is what made key 7 special up to Kafka 0.11.
     */
    public const int HEADER_V0 = 0;

    /**
     * Request header **v1**: api key, api version, correlation id and an int16-prefixed client id
     *
     * The header of every api of Kafka 0.8 to 2.3, and of every non-flexible version afterwards.
     */
    public const int HEADER_V1 = 1;

    /**
     * Request header **v2** (KIP-482, Kafka 2.4): the header v1 plus a tagged-field section
     *
     * The `client_id` keeps its int16 length prefix even here - `RequestHeader.json` @ 2.8.2 marks the field
     * `"flexibleVersions": "none"`, so that an old broker can read the header of any ApiVersions request - and the
     * tag buffer is empty on every frame this fixture sends. A broker derives the header version from the
     * **requested** api version (`RequestHeader.parse` @ 2.8.2), so a flexible version sent with the header v1 is a
     * parse failure and costs the connection.
     */
    public const int HEADER_V2 = 2;

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
     * @param int    $headerVersion Version of the request header: {@see self::HEADER_V0} without a client id,
     *                              {@see self::HEADER_V1} with one, {@see self::HEADER_V2} with a tag buffer on top
     *
     * @return array{status: string, correlationId: int|null, body: string}
     */
    public function send(
        int $apiKey,
        int $apiVersion,
        string $body,
        int $correlationId,
        int $headerVersion = self::HEADER_V1
    ): array {
        $header = pack('nnN', $apiKey, $apiVersion, $correlationId);
        if ($headerVersion >= self::HEADER_V1) {
            $header .= self::string('kafka-client-t1-probe');
        }
        if ($headerVersion >= self::HEADER_V2) {
            $header .= self::tagBuffer();
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
     * Encodes an int8, which PHP packs as a signed char
     */
    public static function int8(int $value): string
    {
        return pack('c', $value);
    }

    /**
     * Encodes an int16 in the big endian order of the protocol
     */
    public static function int16(int $value): string
    {
        return pack('n', $value);
    }

    /**
     * Encodes a boolean: the single byte `00` or `01`
     */
    public static function boolean(bool $value): string
    {
        return $value ? "\x01" : "\x00";
    }

    /**
     * Encodes an **unsigned varint**: 7 bits per byte, least significant group first, the high bit set on every
     * byte but the last, and no zigzag step (`ByteUtils.writeUnsignedVarint` @ 2.8.2)
     *
     * This is the counter of every compact type of KIP-482, and the one piece of the flexible encoding that the
     * record format of 0.11 does not already have - the varints of a record batch are zigzag-encoded, these are not.
     */
    public static function unsignedVarint(int $value): string
    {
        $bytes = '';
        while (($value & ~0x7F) !== 0) {
            $bytes .= chr(($value & 0x7F) | 0x80);
            $value = ($value >> 7) & (PHP_INT_MAX >> 6);
        }

        return $bytes . chr($value);
    }

    /**
     * Encodes a compact string: the unsigned varint `length + 1` followed by the bytes, `00` for `null`
     */
    public static function compactString(?string $value): string
    {
        return $value === null ? self::unsignedVarint(0) : self::unsignedVarint(strlen($value) + 1) . $value;
    }

    /**
     * Encodes a compact byte array, which is the encoding of a compact string
     */
    public static function compactBytes(?string $value): string
    {
        return self::compactString($value);
    }

    /**
     * Encodes the element count of a compact array: the unsigned varint `count + 1`, `00` for a null array
     */
    public static function compactArray(?int $count): string
    {
        return $count === null ? self::unsignedVarint(0) : self::unsignedVarint($count + 1);
    }

    /**
     * Encodes an empty tagged-field section: the unsigned varint `0`
     *
     * Every structure of a flexible version ends with one - the body of the request, the request header v2 and
     * every nested structure alike - and a frame that leaves it out is dropped with the connection.
     */
    public static function tagBuffer(): string
    {
        return "\x00";
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
