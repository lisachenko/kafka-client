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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;

/**
 * Establishes which api keys and versions a real Kafka 0.9.0.1 broker serves.
 *
 * Kafka 0.9 has no ApiVersions request (that is key 18, Kafka 0.10), so the only way to know the protocol surface of
 * a broker is to send a minimal well-formed request of every key and version and to watch what comes back. The
 * result is the api-key table of `docs/protocol/0.10.2.md`, and this test keeps that table honest.
 *
 * Two properties of the 0.9.0.1 broker make the probe possible at all:
 *
 * * A request it can not parse is neither answered nor rejected. The network thread logs "Processor got uncaught
 *   exception" and drops it, and the connection keeps working - a client that sends one simply waits for its own
 *   timeout, which is what {@see RawApiProbe::SILENT} means below.
 * * The apis that a 0.9 broker still parses with the Scala classes of the 0.8 line (keys 0-9) do not check the
 *   version they are sent with, so they answer versions that do not exist. The version *is* checked for the apis
 *   that go through the Java `Protocol` schemas (keys 10-16) and for the two Scala apis that assert it explicitly
 *   (OffsetCommit and UpdateMetadata).
 *
 * The probe never changes the state of the cluster: the broker-to-broker apis are sent with the stale controller
 * epoch -1 and empty partition sets, ControlledShutdown asks for a broker id that does not exist, and the group
 * apis use a group id that no other test uses and are answered with an error before a group is created.
 *
 * @see docs/protocol/0.10.2.md, section "API keys"
 */
#[CoversClass(ApiKeys::class)]
final class ApiVersionProbeTest extends IntegrationTestCase
{
    /**
     * An unknown broker id, so that ControlledShutdown reports an error instead of stopping a broker
     */
    private const int UNKNOWN_BROKER_ID = 4242;

    /**
     * Group id of this test class; every group request of the probe fails before a group is created
     */
    private string $groupId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->groupId = 't1-probe-' . bin2hex(random_bytes(6));
    }

    /**
     * Every api key and version that Kafka 0.9.0.1 serves
     *
     * @return array<string, array{int, int}>
     */
    public static function servedApiProvider(): array
    {
        return [
            'Produce v0'            => [ApiKeys::PRODUCE, 0],
            'Produce v1'            => [ApiKeys::PRODUCE, 1],
            'Fetch v0'              => [ApiKeys::FETCH, 0],
            'Fetch v1'              => [ApiKeys::FETCH, 1],
            'Offsets v0'            => [ApiKeys::OFFSETS, 0],
            'Metadata v0'           => [ApiKeys::METADATA, 0],
            'LeaderAndIsr v0'       => [ApiKeys::LEADER_AND_ISR, 0],
            'StopReplica v0'        => [ApiKeys::STOP_REPLICA, 0],
            'UpdateMetadata v0'     => [ApiKeys::UPDATE_METADATA, 0],
            'UpdateMetadata v1'     => [ApiKeys::UPDATE_METADATA, 1],
            'ControlledShutdown v0' => [ApiKeys::CONTROLLED_SHUTDOWN, 0],
            'ControlledShutdown v1' => [ApiKeys::CONTROLLED_SHUTDOWN, 1],
            'OffsetCommit v0'       => [ApiKeys::OFFSET_COMMIT, 0],
            'OffsetCommit v1'       => [ApiKeys::OFFSET_COMMIT, 1],
            'OffsetCommit v2'       => [ApiKeys::OFFSET_COMMIT, 2],
            'OffsetFetch v0'        => [ApiKeys::OFFSET_FETCH, 0],
            'OffsetFetch v1'        => [ApiKeys::OFFSET_FETCH, 1],
            'GroupCoordinator v0'   => [ApiKeys::GROUP_COORDINATOR, 0],
            'JoinGroup v0'          => [ApiKeys::JOIN_GROUP, 0],
            'Heartbeat v0'          => [ApiKeys::HEARTBEAT, 0],
            'LeaveGroup v0'         => [ApiKeys::LEAVE_GROUP, 0],
            'SyncGroup v0'          => [ApiKeys::SYNC_GROUP, 0],
            'DescribeGroups v0'     => [ApiKeys::DESCRIBE_GROUPS, 0],
            'ListGroups v0'         => [ApiKeys::LIST_GROUPS, 0],
        ];
    }

    #[DataProvider('servedApiProvider')]
    public function testTheBrokerAnswersEveryApiOfKafka0901(int $apiKey, int $apiVersion): void
    {
        $correlationId = 1000 + $apiKey * 10 + $apiVersion;
        $result        = $this->probe($apiKey, $apiVersion, $correlationId);

        self::assertSame(
            RawApiProbe::ANSWERED,
            $result['status'],
            "The broker did not answer the api key {$apiKey} version {$apiVersion}"
        );
        self::assertSame($correlationId, $result['correlationId']);
    }

    /**
     * The versions and keys that arrived after Kafka 0.9.0.1, plus the first version above every checked api
     *
     * @return array<string, array{int, int}>
     */
    public static function unservedApiProvider(): array
    {
        return [
            'UpdateMetadata v2' => [ApiKeys::UPDATE_METADATA, 2],
            'OffsetCommit v3'   => [ApiKeys::OFFSET_COMMIT, 3],
            'GroupCoordinator v1 (0.10)' => [ApiKeys::GROUP_COORDINATOR, 1],
            'JoinGroup v1 (0.10.1)'      => [ApiKeys::JOIN_GROUP, 1],
            'Heartbeat v1 (0.11)'        => [ApiKeys::HEARTBEAT, 1],
            'LeaveGroup v1 (0.11)'       => [ApiKeys::LEAVE_GROUP, 1],
            'SyncGroup v1 (0.10.1)'      => [ApiKeys::SYNC_GROUP, 1],
            'DescribeGroups v1 (0.11)'   => [ApiKeys::DESCRIBE_GROUPS, 1],
            'ListGroups v1 (0.11)'       => [ApiKeys::LIST_GROUPS, 1],
            'SaslHandshake (17, 0.10)'   => [17, 0],
            'ApiVersions (18, 0.10)'     => [18, 0],
        ];
    }

    #[DataProvider('unservedApiProvider')]
    public function testTheBrokerDropsAnApiItDoesNotKnow(int $apiKey, int $apiVersion): void
    {
        $result = $this->probe($apiKey, $apiVersion, 2000 + $apiKey * 10 + $apiVersion);

        self::assertSame(
            RawApiProbe::SILENT,
            $result['status'],
            "The broker answered the api key {$apiKey} version {$apiVersion}, which Kafka 0.9.0.1 does not have"
        );
    }

    /**
     * A dropped request does not cost the connection, it only costs the answer
     */
    public function testADroppedRequestLeavesTheConnectionUsable(): void
    {
        $probe = new RawApiProbe(self::firstBootstrapServer());

        $dropped = $probe->send(18, 0, $this->body(18, 0), 3001);
        $served  = $probe->send(ApiKeys::METADATA, 0, $this->body(ApiKeys::METADATA, 0), 3002);
        $probe->close();

        self::assertSame(RawApiProbe::SILENT, $dropped['status'], 'ApiVersions is a Kafka 0.10 api key');
        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(3002, $served['correlationId'], 'the answer of the dropped request never arrives');
    }

    /**
     * The apis that a 0.9 broker parses with the Scala classes of the 0.8 line accept any version they are sent
     *
     * @return array<string, array{int, int}>
     */
    public static function unvalidatedVersionProvider(): array
    {
        return [
            'Produce v2'            => [ApiKeys::PRODUCE, 2],
            'Fetch v2'              => [ApiKeys::FETCH, 2],
            'Offsets v1'            => [ApiKeys::OFFSETS, 1],
            'Metadata v1'           => [ApiKeys::METADATA, 1],
            'ControlledShutdown v2' => [ApiKeys::CONTROLLED_SHUTDOWN, 2],
            'OffsetFetch v2'        => [ApiKeys::OFFSET_FETCH, 2],
        ];
    }

    #[DataProvider('unvalidatedVersionProvider')]
    public function testTheScalaApisAnswerAVersionThatKafka0901DoesNotHave(int $apiKey, int $apiVersion): void
    {
        $result = $this->probe($apiKey, $apiVersion, 4000 + $apiKey * 10 + $apiVersion);

        self::assertSame(
            RawApiProbe::ANSWERED,
            $result['status'],
            "The api key {$apiKey} refused the version {$apiVersion}, so it does validate its version after all"
        );
    }

    /**
     * Produce and Fetch are the two apis whose response grew a throttle time field in version 1
     */
    public function testVersionOneOfProduceAndFetchAnswersWithTheThrottleTimeField(): void
    {
        $emptyProduceV0 = $this->probe(ApiKeys::PRODUCE, 0, 5000)['body'];
        $emptyProduceV1 = $this->probe(ApiKeys::PRODUCE, 1, 5001)['body'];
        $emptyFetchV0   = $this->probe(ApiKeys::FETCH, 0, 5002)['body'];
        $emptyFetchV1   = $this->probe(ApiKeys::FETCH, 1, 5003)['body'];

        // An empty topic array is four zero bytes, the throttle time of an unthrottled client four more
        self::assertSame(4, strlen($emptyProduceV0));
        self::assertSame(8, strlen($emptyProduceV1));
        self::assertSame(4, strlen($emptyFetchV0));
        self::assertSame(8, strlen($emptyFetchV1));
    }

    /**
     * The group apis of 0.9 report the error codes that this branch gained together with them
     */
    public function testGroupApisReportTheErrorCodesOfKafka09(): void
    {
        $joinGroup = $this->probe(ApiKeys::JOIN_GROUP, 0, 6001);
        $heartbeat = $this->probe(ApiKeys::HEARTBEAT, 0, 6002);

        // The probe joins with a session timeout of 1 ms, which is below group.min.session.timeout.ms
        self::assertContains(
            self::errorCodeOf($joinGroup['body']),
            [
                KafkaException::INVALID_SESSION_TIMEOUT,
                KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
                KafkaException::GROUP_LOAD_IN_PROGRESS,
            ],
            'JoinGroup with a session timeout below the minimum of the broker'
        );
        // Heartbeat of a member of a group that does not exist
        self::assertContains(
            self::errorCodeOf($heartbeat['body']),
            [
                KafkaException::UNKNOWN_MEMBER_ID,
                KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
                KafkaException::GROUP_LOAD_IN_PROGRESS,
            ],
            'Heartbeat for a member the coordinator does not know'
        );
    }

    /**
     * Sends one probe request on a connection of its own and returns the result
     *
     * @return array{status: string, correlationId: int|null, body: string}
     */
    private function probe(int $apiKey, int $apiVersion, int $correlationId): array
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        // Version 0 of ControlledShutdown is the one api of the 0.8 line whose header has no client id
        $result = $probe->send(
            $apiKey,
            $apiVersion,
            $this->body($apiKey, $apiVersion),
            $correlationId,
            !($apiKey === ApiKeys::CONTROLLED_SHUTDOWN && $apiVersion === 0)
        );
        $probe->close();

        return $result;
    }

    /**
     * Builds the smallest well-formed body of the given api that changes nothing on the cluster
     */
    private function body(int $apiKey, int $apiVersion): string
    {
        $emptyArray   = RawApiProbe::int32(0);
        $staleControl = RawApiProbe::int32(-1) . RawApiProbe::int32(-1);

        return match ($apiKey) {
            // RequiredAcks 1, a timeout and no topic at all
            ApiKeys::PRODUCE  => pack('n', 1) . RawApiProbe::int32(1000) . $emptyArray,
            // ReplicaId -1 (an ordinary consumer), MaxWaitTime, MinBytes and no topic
            ApiKeys::FETCH    => RawApiProbe::int32(-1) . RawApiProbe::int32(100) . RawApiProbe::int32(0)
                . $emptyArray,
            ApiKeys::OFFSETS  => RawApiProbe::int32(-1) . $emptyArray,
            ApiKeys::METADATA => $emptyArray,
            // The broker-to-broker apis: a controller epoch of -1 is stale, so the broker answers 11 and does nothing
            ApiKeys::LEADER_AND_ISR      => $staleControl . $emptyArray . $emptyArray,
            ApiKeys::STOP_REPLICA        => $staleControl . pack('C', 0) . $emptyArray,
            ApiKeys::UPDATE_METADATA     => $staleControl . $emptyArray . $emptyArray,
            ApiKeys::CONTROLLED_SHUTDOWN => RawApiProbe::int32(self::UNKNOWN_BROKER_ID),
            ApiKeys::OFFSET_COMMIT       => match ($apiVersion) {
                0       => RawApiProbe::string($this->groupId) . $emptyArray,
                1       => RawApiProbe::string($this->groupId) . RawApiProbe::int32(-1) . RawApiProbe::string('')
                    . $emptyArray,
                default => RawApiProbe::string($this->groupId) . RawApiProbe::int32(-1) . RawApiProbe::string('')
                    . RawApiProbe::int64(-1) . $emptyArray,
            },
            ApiKeys::OFFSET_FETCH      => RawApiProbe::string($this->groupId) . $emptyArray,
            ApiKeys::GROUP_COORDINATOR => RawApiProbe::string($this->groupId),
            // A session timeout of 1 ms is below group.min.session.timeout.ms, so no group is created
            ApiKeys::JOIN_GROUP        => RawApiProbe::string($this->groupId) . RawApiProbe::int32(1)
                . RawApiProbe::string('') . RawApiProbe::string('consumer') . $emptyArray,
            ApiKeys::HEARTBEAT         => RawApiProbe::string($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::string('probe-member'),
            ApiKeys::LEAVE_GROUP       => RawApiProbe::string($this->groupId) . RawApiProbe::string('probe-member'),
            ApiKeys::SYNC_GROUP        => RawApiProbe::string($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::string('probe-member') . $emptyArray,
            ApiKeys::DESCRIBE_GROUPS   => $emptyArray,
            ApiKeys::LIST_GROUPS       => '',
            // SaslHandshake (17) of Kafka 0.10 asks for a mechanism, ApiVersions (18) has an empty body
            17                         => RawApiProbe::string('GSSAPI'),
            default                    => '',
        };
    }

    /**
     * Reads the error code that the group apis put in front of their response body
     */
    private static function errorCodeOf(string $body): int
    {
        self::assertGreaterThanOrEqual(2, strlen($body), 'the response carries no error code');

        return (int) unpack('s', strrev(substr($body, 0, 2)))[1];
    }
}
