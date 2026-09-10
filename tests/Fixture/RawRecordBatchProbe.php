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

use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use RuntimeException;

/**
 * Hand-built Produce and Fetch frames, so that the message format v2 can be verified without the apis that carry it.
 *
 * The record batch v2 arrives on the wire through Produce v3 and Fetch v4/v5, and the classes of those versions are
 * not part of this ticket - they are written on top of the format, not the other way round. Everything the format
 * needs from a broker is therefore asked for with raw frames here: a batch this client built is written into a
 * partition with a Produce v3 frame and read back with a Fetch v4 frame, which is the only way to see what the
 * broker did with it. The same probe asks for the **down-conversions**, by fetching a log of the message format v2
 * with a Fetch v3 (which answers the message format v1) and with a Fetch v1 (which answers the format v0).
 *
 * The transaction apis are here for the same reason: a control batch is written by the broker itself, and the only
 * way to make it write one without a transactional producer is the raw sequence InitProducerId (22),
 * AddPartitionsToTxn (24), Produce v3 with a transactional id and EndTxn (26). The classes of those apis belong to
 * the tickets of the idempotent and the transactional producer.
 *
 * The request bodies are the schemas of `Protocol.java` @ 0.11.0.3; the responses are read with the primitive types
 * of the engine, field by field, because there is no response class to decode them into yet.
 *
 * @see \Protocol\Kafka\Tests\Fixture\RawApiProbe for the probe that asks what a broker does with a frame it refuses
 * @see docs/protocol/1.1.md, section "RecordBatch (message format v2)"
 */
final class RawRecordBatchProbe
{
    /**
     * Api key of the Produce api
     */
    public const int PRODUCE = 0;

    /**
     * Api key of the Fetch api
     */
    public const int FETCH = 1;

    /**
     * Api key of the GroupCoordinator api, which Kafka 0.11 also uses to find a transaction coordinator
     */
    public const int GROUP_COORDINATOR = 10;

    /**
     * Api key of the InitProducerId api of Kafka 0.11
     */
    public const int INIT_PRODUCER_ID = 22;

    /**
     * Api key of the AddPartitionsToTxn api of Kafka 0.11
     */
    public const int ADD_PARTITIONS_TO_TXN = 24;

    /**
     * Api key of the EndTxn api of Kafka 0.11
     */
    public const int END_TXN = 26;

    /**
     * How long to wait for a response frame before giving up, in seconds
     */
    private const float RESPONSE_TIMEOUT = 15.0;

    /**
     * Open connection to the broker
     *
     * @var resource
     */
    private $socket;

    /**
     * Correlation id of the next request
     */
    private int $correlationId = 1;

    /**
     * Frame of the last request that was sent, without its Size field, for the vectors that document it
     */
    private string $lastRequestFrame = '';

    /**
     * @param string $address  Broker address as `host:port`
     * @param string $clientId Client id of every frame this probe sends
     */
    public function __construct(
        string $address,
        private readonly string $clientId = 't2-vectors',
        float $connectTimeout = 5.0,
    ) {
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
     * Writes a byte region into a partition with a Produce v3 frame and returns what the broker answered
     *
     * @param string      $recordSet       The bytes of the record batch, exactly as they go on the wire
     * @param string|null $transactionalId Transactional id of the producer, null for everything else
     *
     * @return array{throttleTimeMs: int, errorCode: int, baseOffset: int, logAppendTime: int}
     */
    public function produce(
        string $topic,
        int $partition,
        string $recordSet,
        ?string $transactionalId = null,
        int $acks = -1,
        int $timeoutMs = 5000,
    ): array {
        $body = self::nullableString($transactionalId)
            . pack('nN', $acks, $timeoutMs)
            . pack('N', 1) . self::string($topic)
            . pack('N', 1) . pack('N', $partition) . pack('N', strlen($recordSet)) . $recordSet;

        $response = new StringStream($this->send(self::PRODUCE, 3, $body));
        self::skipArrayOfOne($response);
        self::readString($response);
        self::skipArrayOfOne($response);

        return [
            'partition'      => self::readInt32($response),
            'errorCode'      => self::readInt16($response),
            'baseOffset'     => self::readInt64($response),
            'logAppendTime'  => self::readInt64($response),
            'throttleTimeMs' => self::readInt32($response),
        ];
    }

    /**
     * Reads a partition back with a Fetch frame of the given version and returns the byte region it answered
     *
     * @param int $version        4 and 5 answer the message format v2, 2 and 3 convert the log down to the format v1
     *                            and 0 and 1 down to the format v0
     * @param int $isolationLevel 0 `read_uncommitted`, 1 `read_committed`; only versions 4 and 5 carry it
     *
     * @return array{throttleTimeMs: int, errorCode: int, highWaterMarkOffset: int, lastStableOffset: int,
     *               abortedTransactions: list<array{producerId: int, firstOffset: int}>|null, recordSet: string,
     *               requestFrame: string}
     */
    public function fetch(
        string $topic,
        int $partition,
        int $fetchOffset = 0,
        int $version = 4,
        int $isolationLevel = 0,
        int $maxBytes = 1048576,
        int $maxWaitMs = 1000,
    ): array {
        $body = pack('NNN', -1, $maxWaitMs, 0);
        if ($version >= 3) {
            $body .= pack('N', $maxBytes);
        }
        if ($version >= 4) {
            $body .= pack('c', $isolationLevel);
        }
        $body .= pack('N', 1) . self::string($topic)
            . pack('N', 1) . pack('N', $partition) . pack('J', $fetchOffset);
        if ($version >= 5) {
            $body .= pack('J', -1);
        }
        $body .= pack('N', $maxBytes);

        $response       = new StringStream($this->send(self::FETCH, $version, $body));
        $throttleTimeMs = $version >= 1 ? self::readInt32($response) : 0;
        self::skipArrayOfOne($response);
        self::readString($response);
        self::skipArrayOfOne($response);

        $result = [
            'throttleTimeMs'      => $throttleTimeMs,
            'partition'           => self::readInt32($response),
            'errorCode'           => self::readInt16($response),
            'highWaterMarkOffset' => self::readInt64($response),
            'lastStableOffset'    => -1,
            'abortedTransactions' => null,
        ];
        if ($version >= 4) {
            $result['lastStableOffset'] = self::readInt64($response);
            $abortedCount               = self::readInt32($response);
            if ($abortedCount >= 0) {
                $aborted = [];
                for ($index = 0; $index < $abortedCount; $index++) {
                    $aborted[] = [
                        'producerId'  => self::readInt64($response),
                        'firstOffset' => self::readInt64($response),
                    ];
                }
                $result['abortedTransactions'] = $aborted;
            }
        }
        if ($version >= 5) {
            $result['logStartOffset'] = self::readInt64($response);
        }

        $result['recordSet']    = (string) BinarySchema::readSingleType(BinarySchema::TYPE_BYTEARRAY, $response);
        $result['requestFrame'] = $this->lastRequestFrame;

        return $result;
    }

    /**
     * Asks for the coordinator of a transactional id with a GroupCoordinator v1 frame (`coordinator_type` 1).
     *
     * A transactional id has to be looked up before its producer id can be asked for: the lookup is what creates the
     * internal `__transaction_state` topic and elects the leaders of its partitions, and until it has happened even
     * the only broker of a one-broker cluster answers InitProducerId with **16 NotCoordinatorForGroup**.
     *
     * @return array{throttleTimeMs: int, errorCode: int, errorMessage: string|null, nodeId: int, host: string,
     *               port: int}
     */
    public function findTransactionCoordinator(string $transactionalId): array
    {
        $body     = self::string($transactionalId) . pack('c', 1);
        $response = new StringStream($this->send(self::GROUP_COORDINATOR, 1, $body));

        return [
            'throttleTimeMs' => self::readInt32($response),
            'errorCode'      => self::readInt16($response),
            'errorMessage'   => BinarySchema::readSingleType(BinarySchema::TYPE_NULLABLE_STRING, $response),
            'nodeId'         => self::readInt32($response),
            'host'           => self::readString($response),
            'port'           => self::readInt32($response),
        ];
    }

    /**
     * Asks the broker for a producer id, the first step of both the idempotent and the transactional producer
     *
     * @param string|null $transactionalId Transactional id, or null for a plain idempotent producer
     *
     * @return array{throttleTimeMs: int, errorCode: int, producerId: int, producerEpoch: int}
     */
    public function initProducerId(?string $transactionalId, int $transactionTimeoutMs = 60000): array
    {
        $body     = self::nullableString($transactionalId) . pack('N', $transactionTimeoutMs);
        $response = new StringStream($this->send(self::INIT_PRODUCER_ID, 0, $body));

        return [
            'throttleTimeMs' => self::readInt32($response),
            'errorCode'      => self::readInt16($response),
            'producerId'     => self::readInt64($response),
            'producerEpoch'  => self::readInt16($response),
        ];
    }

    /**
     * Registers a partition with the transaction coordinator, which is what makes it write markers into it later
     *
     * @return array{throttleTimeMs: int, errorCode: int}
     */
    public function addPartitionsToTxn(
        string $transactionalId,
        int $producerId,
        int $producerEpoch,
        string $topic,
        int $partition,
    ): array {
        $body = self::string($transactionalId) . pack('J', $producerId) . pack('n', $producerEpoch)
            . pack('N', 1) . self::string($topic) . pack('N', 1) . pack('N', $partition);

        $response       = new StringStream($this->send(self::ADD_PARTITIONS_TO_TXN, 0, $body));
        $throttleTimeMs = self::readInt32($response);
        self::skipArrayOfOne($response);
        self::readString($response);
        self::skipArrayOfOne($response);
        self::readInt32($response);

        return ['throttleTimeMs' => $throttleTimeMs, 'errorCode' => self::readInt16($response)];
    }

    /**
     * Ends the transaction, which makes the coordinator write a control batch into every partition of it
     *
     * @return array{throttleTimeMs: int, errorCode: int}
     */
    public function endTxn(string $transactionalId, int $producerId, int $producerEpoch, bool $commit): array
    {
        $body = self::string($transactionalId) . pack('J', $producerId) . pack('n', $producerEpoch)
            . pack('C', $commit ? 1 : 0);

        $response = new StringStream($this->send(self::END_TXN, 0, $body));

        return [
            'throttleTimeMs' => self::readInt32($response),
            'errorCode'      => self::readInt16($response),
        ];
    }

    /**
     * Returns the frame of the last request that was sent, without its Size field
     */
    public function lastRequestFrame(): string
    {
        return $this->lastRequestFrame;
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
     * Encodes a protocol string: an int16 length followed by its bytes
     */
    private static function string(string $value): string
    {
        return pack('n', strlen($value)) . $value;
    }

    /**
     * Encodes a nullable protocol string, `null` as the length -1
     */
    private static function nullableString(?string $value): string
    {
        return $value === null ? pack('n', 0xFFFF) : self::string($value);
    }

    private static function readInt16(StringStream $stream): int
    {
        return (int) BinarySchema::readSingleType(BinarySchema::TYPE_INT16, $stream);
    }

    private static function readInt32(StringStream $stream): int
    {
        return (int) BinarySchema::readSingleType(BinarySchema::TYPE_INT32, $stream);
    }

    private static function readInt64(StringStream $stream): int
    {
        return (int) BinarySchema::readSingleType(BinarySchema::TYPE_INT64, $stream);
    }

    private static function readString(StringStream $stream): string
    {
        return (string) BinarySchema::readSingleType(BinarySchema::TYPE_STRING, $stream);
    }

    /**
     * Reads the element count of an array that has to hold exactly one item, the shape of every probe request
     */
    private static function skipArrayOfOne(StringStream $stream): void
    {
        $count = self::readInt32($stream);
        if ($count !== 1) {
            throw new RuntimeException("The broker answered with {$count} items where one was expected");
        }
    }

    /**
     * Sends one request frame and returns the body of the response, without the correlation id
     */
    private function send(int $apiKey, int $apiVersion, string $body): string
    {
        $correlationId          = $this->correlationId++;
        $frame                  = pack('nnN', $apiKey, $apiVersion, $correlationId)
            . self::string($this->clientId) . $body;
        $this->lastRequestFrame = $frame;
        fwrite($this->socket, pack('N', strlen($frame)) . $frame);

        $deadline = microtime(true) + self::RESPONSE_TIMEOUT;
        $buffer   = '';
        while (microtime(true) < $deadline) {
            $chunk = fread($this->socket, 65536);
            if (is_string($chunk) && $chunk !== '') {
                $buffer .= $chunk;
            }
            if (strlen($buffer) >= 4) {
                $size = (int) unpack('N', substr($buffer, 0, 4))[1];
                if (strlen($buffer) >= 4 + $size) {
                    $answered = (int) unpack('N', substr($buffer, 4, 4))[1];
                    if ($answered !== $correlationId) {
                        throw new RuntimeException(
                            "The broker answered the correlation id {$answered} instead of {$correlationId}"
                        );
                    }

                    return substr($buffer, 8, $size - 4);
                }
            }
            if (feof($this->socket)) {
                throw new RuntimeException(
                    "The broker closed the connection on the api {$apiKey} v{$apiVersion}; "
                    . 'its log says why it could not parse the frame'
                );
            }
            usleep(10000);
        }

        throw new RuntimeException("The broker did not answer the api {$apiKey} v{$apiVersion} in time");
    }
}
