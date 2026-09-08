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

use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\AbstractStream;

/**
 * A broker connection that answers with scripted responses instead of talking to a real broker.
 *
 * The double behaves the way a broker behaves on one connection: it waits for a complete request frame, takes the
 * next scripted answer and puts the correlation id of that request into its header, so a test never has to predict
 * the value of the process-wide correlation id counter. The answers are handed out in the order the requests
 * arrived, exactly like a broker serves one connection.
 *
 * A script that runs out of answers models a broker that stops answering: the next read fails the way a socket read
 * without data does.
 *
 * @see \Protocol\Kafka\Network\ResponseValidator
 */
final class BrokerConnection extends AbstractStream
{
    /**
     * Bytes that the client can still read
     */
    private string $readBuffer = '';

    /**
     * Bytes the client wrote and that do not form a complete request frame yet
     */
    private string $pendingRequest = '';

    /**
     * Scripted answers, `null` stands for a request the broker never answers
     *
     * @var list<string|null>
     */
    private array $responses;

    /**
     * Correlation ids of every request that arrived here
     *
     * @var list<int>
     */
    private array $receivedCorrelationIds = [];

    /**
     * Payload of every request frame that arrived here, the size prefix excluded
     *
     * @var list<string>
     */
    private array $receivedFrames = [];

    /**
     * Whether the correlation id of the request is copied into the answer, as a broker does it
     */
    private bool $echoesCorrelationId = true;

    /**
     * @param string|null ...$responses Answer for each request, in order
     */
    public function __construct(?string ...$responses)
    {
        $this->responses = array_values($responses);
    }

    /**
     * Makes this connection answer with the correlation id of the script instead of the one of the request
     */
    public function withoutCorrelationIdEcho(): self
    {
        $this->echoesCorrelationId = false;

        return $this;
    }

    public function write(string $format, ...$arguments): void
    {
        $this->pendingRequest .= pack($format, ...$arguments);
        $this->serveCompleteRequests();
    }

    public function read(string $format): array
    {
        $packetSize = self::packetSize($format);
        $available  = strlen($this->readBuffer);
        if ($available < $packetSize) {
            throw new NetworkException(
                ['error' => "Unexpected end of stream: {$packetSize} bytes requested, {$available} available"]
            );
        }

        $arguments        = unpack($format, $this->readBuffer);
        $this->readBuffer = substr($this->readBuffer, $packetSize);
        if ($arguments === false) {
            throw new \InvalidArgumentException("Can not unpack the data with the format: {$format}");
        }

        return $arguments;
    }

    public function isConnected(): bool
    {
        return true;
    }

    public function isEmpty(): bool
    {
        return $this->readBuffer === '';
    }

    /**
     * Returns the correlation id of every request this connection received
     *
     * @return list<int>
     */
    public function getReceivedCorrelationIds(): array
    {
        return $this->receivedCorrelationIds;
    }

    /**
     * Returns the payload of every request frame this connection received, the size prefix excluded
     *
     * @return list<string>
     */
    public function getReceivedFrames(): array
    {
        return $this->receivedFrames;
    }

    /**
     * Returns how many requests this connection received
     */
    public function getRequestCount(): int
    {
        return count($this->receivedCorrelationIds);
    }

    /**
     * Takes every complete request frame out of the write buffer and answers it
     */
    private function serveCompleteRequests(): void
    {
        while (strlen($this->pendingRequest) >= 4) {
            $messageSize = (int) unpack('Nsize', substr($this->pendingRequest, 0, 4))['size'];
            if (strlen($this->pendingRequest) < 4 + $messageSize) {
                return;
            }
            $frame                = substr($this->pendingRequest, 4, $messageSize);
            $this->pendingRequest = substr($this->pendingRequest, 4 + $messageSize);

            // Request header: ApiKey int16, ApiVersion int16, CorrelationId int32, ClientId string
            $correlationId                  = (int) unpack('Nid', substr($frame, 4, 4))['id'];
            $this->receivedCorrelationIds[] = $correlationId;
            $this->receivedFrames[]         = $frame;

            $this->answer($correlationId);
        }
    }

    /**
     * Appends the next scripted answer, with the correlation id of the request in its header
     */
    private function answer(int $correlationId): void
    {
        if ($this->responses === []) {
            return;
        }
        $response = array_shift($this->responses);
        if ($response === null) {
            return;
        }
        if ($this->echoesCorrelationId) {
            // Size int32, CorrelationId int32, then the body of the response
            $response = substr_replace($response, pack('N', $correlationId), 4, 4);
        }

        $this->readBuffer .= $response;
    }
}
