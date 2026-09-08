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
 * Joins a consumer group on a real broker with hand-built JoinGroup and SyncGroup frames.
 *
 * The admin apis of Kafka 0.9 - ListGroups and DescribeGroups - only have something to report once a group really
 * exists on the coordinator, and a group only exists while it has members. This helper creates one without the
 * group membership classes of the client, which live on another branch of this line, and without a Java consumer in
 * a container: it speaks the two requests of the membership protocol that are needed to reach the `Stable` state.
 *
 * <pre>
 *   JoinGroupRequest  => GroupId SessionTimeout MemberId ProtocolType [ProtocolName ProtocolMetadata]
 *   JoinGroupResponse => ErrorCode GenerationId GroupProtocol LeaderId MemberId [MemberId MemberMetadata]
 *   SyncGroupRequest  => GroupId GenerationId MemberId [MemberId MemberAssignment]
 *   SyncGroupResponse => ErrorCode MemberAssignment
 * </pre>
 *
 * A member that stops sending heartbeats is only forgotten when its session timeout expires, so the group stays in
 * the state this helper left it in for `sessionTimeoutMs` after the last request - long enough for a test to look at
 * it with the admin apis.
 *
 * @see \Protocol\Kafka\Tests\Integration\AdminGroupApiTest
 */
final class RawGroupMember
{
    /**
     * Protocol type of a consumer group, the only one Kafka itself defines
     */
    public const string PROTOCOL_TYPE = 'consumer';

    /**
     * How long to wait for a response frame, in seconds
     */
    private const float RESPONSE_TIMEOUT = 30.0;

    /**
     * Open connection to the coordinator of the group
     *
     * @var resource
     */
    private $socket;

    /**
     * Member id the coordinator assigned, empty until the first JoinGroup response
     */
    private string $memberId = '';

    /**
     * Generation of the group, -1 until the first JoinGroup response
     */
    private int $generationId = -1;

    /**
     * Protocol the coordinator picked out of the ones the member offered
     */
    private string $groupProtocol = '';

    /**
     * Correlation id sequence of this connection
     */
    private int $correlationId = 0;

    /**
     * @param string $address          Address of the coordinator of the group, as `host:port`
     * @param string $groupId          Group to join
     * @param string $clientId         Client id of the member, which the broker reports back in DescribeGroups
     * @param int    $sessionTimeoutMs How long the coordinator keeps the member without a heartbeat
     */
    public function __construct(
        string $address,
        private readonly string $groupId,
        private readonly string $clientId = 'kafka-client-raw-member',
        private readonly int $sessionTimeoutMs = 30000
    ) {
        $socket = @stream_socket_client('tcp://' . $address, $errorNumber, $errorString, 5.0);
        if ($socket === false) {
            throw new RuntimeException("Can not connect to {$address}: {$errorString} ({$errorNumber})");
        }
        stream_set_timeout($socket, (int) self::RESPONSE_TIMEOUT);

        $this->socket = $socket;
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Joins the group and becomes its leader, as the only member of a fresh group always does
     *
     * @param string $protocolName Name of the assignor the member offers, e.g. `range`
     * @param string $metadata     Opaque protocol metadata, the `Subscription` of a consumer group
     */
    public function join(string $protocolName = 'range', string $metadata = ''): void
    {
        $body = self::string($this->groupId)
            . pack('N', $this->sessionTimeoutMs)
            . self::string($this->memberId)
            . self::string(self::PROTOCOL_TYPE)
            . pack('N', 1) . self::string($protocolName) . self::bytes($metadata);

        $response = $this->send(11, $body);

        $errorCode = self::readInt16($response);
        if ($errorCode !== 0) {
            throw new RuntimeException("JoinGroup for the group {$this->groupId} failed with {$errorCode}");
        }
        $this->generationId  = self::readInt32($response);
        $this->groupProtocol = self::readString($response);
        self::readString($response); // LeaderId, which is this member for a fresh group
        $this->memberId = self::readString($response);
    }

    /**
     * Publishes the assignment of the group as its leader and reaches the `Stable` state
     *
     * @param string $assignment Opaque assignment for this member, the `MemberAssignment` of a consumer group
     */
    public function sync(string $assignment = ''): void
    {
        $body = self::string($this->groupId)
            . pack('N', $this->generationId)
            . self::string($this->memberId)
            . pack('N', 1) . self::string($this->memberId) . self::bytes($assignment);

        $response  = $this->send(14, $body);
        $errorCode = self::readInt16($response);
        if ($errorCode !== 0) {
            throw new RuntimeException("SyncGroup for the group {$this->groupId} failed with {$errorCode}");
        }
    }

    /**
     * Joins the group and immediately assigns everything to itself, leaving the group `Stable`
     */
    public function joinAndSync(string $protocolName = 'range', string $metadata = '', string $assignment = ''): void
    {
        $this->join($protocolName, $metadata);
        $this->sync($assignment);
    }

    public function getMemberId(): string
    {
        return $this->memberId;
    }

    public function getGenerationId(): int
    {
        return $this->generationId;
    }

    public function getGroupProtocol(): string
    {
        return $this->groupProtocol;
    }

    public function getClientId(): string
    {
        return $this->clientId;
    }

    public function getGroupId(): string
    {
        return $this->groupId;
    }

    /**
     * Leaves the group with a LeaveGroup request, so that the next test does not have to wait for the session timeout
     */
    public function leave(): void
    {
        if ($this->memberId === '' || !is_resource($this->socket)) {
            return;
        }
        $this->send(13, self::string($this->groupId) . self::string($this->memberId));
        $this->memberId = '';
    }

    /**
     * Closes the connection to the coordinator
     */
    public function close(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }
    }

    /**
     * Sends one request and returns the body of its response, the correlation id stripped
     *
     * @param int    $apiKey Api key of the request
     * @param string $body   Request body, everything after the header
     */
    private function send(int $apiKey, string $body): string
    {
        $correlationId = $this->correlationId++;
        $frame         = pack('nnN', $apiKey, 0, $correlationId) . self::string($this->clientId) . $body;
        fwrite($this->socket, pack('N', strlen($frame)) . $frame);

        $size     = (int) unpack('N', $this->readExactly(4))[1];
        $response = $this->readExactly($size);

        $echoed = (int) unpack('N', substr($response, 0, 4))[1];
        if ($echoed !== $correlationId) {
            throw new RuntimeException("The broker answered the correlation id {$echoed}, not {$correlationId}");
        }

        return substr($response, 4);
    }

    /**
     * Reads exactly the given number of bytes from the connection
     */
    private function readExactly(int $length): string
    {
        $buffer   = '';
        $deadline = microtime(true) + self::RESPONSE_TIMEOUT;
        while (strlen($buffer) < $length) {
            if (microtime(true) > $deadline) {
                throw new RuntimeException("The broker did not answer within " . self::RESPONSE_TIMEOUT . ' seconds');
            }
            $chunk = fread($this->socket, $length - strlen($buffer));
            if ($chunk === false || ($chunk === '' && feof($this->socket))) {
                throw new RuntimeException('The broker closed the connection');
            }
            $buffer .= $chunk;
        }

        return $buffer;
    }

    /**
     * Encodes a protocol string: an int16 length followed by its bytes
     */
    private static function string(string $value): string
    {
        return pack('n', strlen($value)) . $value;
    }

    /**
     * Encodes a protocol byte array: an int32 length followed by its bytes
     */
    private static function bytes(string $value): string
    {
        return pack('N', strlen($value)) . $value;
    }

    /**
     * Reads a signed int16 off the front of the buffer
     */
    private static function readInt16(string &$buffer): int
    {
        $value  = (int) unpack('n', substr($buffer, 0, 2))[1];
        $buffer = substr($buffer, 2);

        return $value >= 0x8000 ? $value - 0x10000 : $value;
    }

    /**
     * Reads a signed int32 off the front of the buffer
     */
    private static function readInt32(string &$buffer): int
    {
        $value  = (int) unpack('N', substr($buffer, 0, 4))[1];
        $buffer = substr($buffer, 4);

        return $value >= 0x80000000 ? $value - 0x100000000 : $value;
    }

    /**
     * Reads a protocol string off the front of the buffer
     */
    private static function readString(string &$buffer): string
    {
        $length = self::readInt16($buffer);
        $value  = $length > 0 ? substr($buffer, 0, $length) : '';
        $buffer = substr($buffer, max($length, 0));

        return $value;
    }
}
