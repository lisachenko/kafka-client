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
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;

/**
 * Establishes which api keys and versions a real Kafka 0.10.2.2 broker serves.
 *
 * Kafka 0.10.0 added the api that answers that question - **ApiVersions**, key 18 - so this class no longer has to
 * guess it the way the `0.8.x` and `0.9.x` lines did. The first half of the suite asks the broker with
 * {@see Client::apiVersions()} and pins its answer, which is the api-key table of `docs/protocol/0.11.0.md`.
 *
 * The second half is still a raw probe ({@see RawApiProbe}), because the *edges* of that table are not in it: what
 * the broker does with a key or a version it does not serve is behaviour, not data. Kafka 0.10 changed that
 * behaviour, and this is where the change is verified:
 *
 * * A 0.9.0.1 broker **dropped** a frame it could not parse and kept the connection open, so a client waited for its
 *   own timeout ({@see RawApiProbe::SILENT}).
 * * A 0.10.2.2 broker **closes the connection** ({@see RawApiProbe::CLOSED}). `SocketServer.processCompletedReceives`
 *   @ 0.10.2.2 catches the `InvalidRequestException` that `RequestChannel.Request` throws for an api key or version
 *   `AbstractRequest.getRequest()` does not know, and the `SchemaException` of a body that does not match the
 *   schema, and calls `close()` on the channel; `docker logs kafka-0-10-2-2` shows
 *   `ERROR Closing socket for ... because of error` with the reason.
 *
 * Two apis are exceptions to that rule and both are verified here: ApiVersions itself, which answers an unknown
 * version with the error code 35, and ControlledShutdown, the last api that a 0.10.2.2 broker still parses with the
 * Scala class of the 0.8 line, which answers every version.
 *
 * The probe never changes the state of the cluster: the broker-to-broker apis are sent with the stale controller
 * epoch -1 and empty partition sets, ControlledShutdown asks for a broker id that does not exist, the group apis use
 * a group id that no other test uses, and CreateTopics/DeleteTopics are sent with an empty topic array.
 *
 * @see docs/protocol/0.11.0.md, section "API keys"
 */
#[CoversClass(ApiKeys::class)]
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
final class ApiVersionProbeTest extends IntegrationTestCase
{
    /**
     * An unknown broker id, so that ControlledShutdown reports an error instead of stopping a broker
     */
    private const int UNKNOWN_BROKER_ID = 4242;

    /**
     * The api table of Kafka 0.10.2.2, as `api key => [minimum version, maximum version]`
     *
     * This is what `Protocol.CURR_VERSION` @ 0.10.2.2 declares and what the container really answers. Key 7 is the
     * only entry whose minimum is not 0: ControlledShutdown v0 uses a request header without a client id, which the
     * Java client cannot build any more.
     */
    private const array SERVED_APIS = [
        ApiKeys::PRODUCE             => [0, 2],
        ApiKeys::FETCH               => [0, 3],
        ApiKeys::OFFSETS             => [0, 1],
        ApiKeys::METADATA            => [0, 2],
        ApiKeys::LEADER_AND_ISR      => [0, 0],
        ApiKeys::STOP_REPLICA        => [0, 0],
        ApiKeys::UPDATE_METADATA     => [0, 3],
        ApiKeys::CONTROLLED_SHUTDOWN => [1, 1],
        ApiKeys::OFFSET_COMMIT       => [0, 2],
        ApiKeys::OFFSET_FETCH        => [0, 2],
        ApiKeys::GROUP_COORDINATOR   => [0, 0],
        ApiKeys::JOIN_GROUP          => [0, 1],
        ApiKeys::HEARTBEAT           => [0, 0],
        ApiKeys::LEAVE_GROUP         => [0, 0],
        ApiKeys::SYNC_GROUP          => [0, 0],
        ApiKeys::DESCRIBE_GROUPS     => [0, 0],
        ApiKeys::LIST_GROUPS         => [0, 0],
        ApiKeys::SASL_HANDSHAKE      => [0, 0],
        ApiKeys::API_VERSIONS        => [0, 0],
        ApiKeys::CREATE_TOPICS       => [0, 1],
        ApiKeys::DELETE_TOPICS       => [0, 0],
    ];

    /**
     * Cluster of this test class, resolved once
     */
    private static ?Cluster $sharedCluster = null;

    /**
     * Group id of this test class; every group request of the probe fails before a group is created
     */
    private string $groupId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->groupId = 't1-probe-' . bin2hex(random_bytes(6));
    }

    public function testTheBrokerReportsEveryApiOfKafka01022(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);

        $reported = [];
        foreach ($response->apiVersions as $apiKey => $metadata) {
            $reported[$apiKey] = [$metadata->minVersion, $metadata->maxVersion];
        }

        self::assertSame(self::SERVED_APIS, $reported, 'the api table of the protocol document');
    }

    public function testTheAdminClientReportsTheSameTable(): void
    {
        $apiVersions = $this->admin()->getApiVersions($this->anyNode());

        self::assertSame(array_keys(self::SERVED_APIS), array_keys($apiVersions));
        self::assertSame(ApiKeys::API_VERSIONS, $apiVersions[ApiKeys::API_VERSIONS]->apiKey);
        self::assertSame(1, $apiVersions[ApiKeys::CONTROLLED_SHUTDOWN]->minVersion);
    }

    public function testTheAnswerIsIndexedByApiKey(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertTrue($response->supports(ApiKeys::FETCH, 3), 'Fetch v3 arrived with Kafka 0.10.1');
        self::assertFalse($response->supports(ApiKeys::FETCH, 4), 'Fetch v4 is Kafka 0.11');
        self::assertFalse($response->supports(ApiKeys::CONTROLLED_SHUTDOWN, 0), 'the minimum version is inclusive');
        self::assertSame(2, $response->maxVersionOf(ApiKeys::METADATA));
        self::assertNull($response->maxVersionOf(21), 'DeleteRecords (21) is Kafka 0.11');
    }

    /**
     * Every api key and version that the table above claims, sent as a real frame
     *
     * @return array<string, array{int, int}>
     */
    public static function servedApiProvider(): array
    {
        $cases = [];
        foreach (self::SERVED_APIS as $apiKey => [$minVersion, $maxVersion]) {
            for ($version = $minVersion; $version <= $maxVersion; $version++) {
                $cases["key {$apiKey} v{$version}"] = [$apiKey, $version];
            }
        }

        return $cases;
    }

    #[DataProvider('servedApiProvider')]
    public function testTheBrokerAnswersEveryApiItReports(int $apiKey, int $apiVersion): void
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
     * The first version above every api of the table, plus the first api key above it
     *
     * @return array<string, array{int, int}>
     */
    public static function unservedApiProvider(): array
    {
        return [
            'Produce v3 (0.11)'            => [ApiKeys::PRODUCE, 3],
            'Fetch v4 (0.11)'              => [ApiKeys::FETCH, 4],
            'Offsets v2 (0.11)'            => [ApiKeys::OFFSETS, 2],
            'Metadata v3 (0.11)'           => [ApiKeys::METADATA, 3],
            'LeaderAndIsr v1 (0.11)'       => [ApiKeys::LEADER_AND_ISR, 1],
            'StopReplica v1 (0.11)'        => [ApiKeys::STOP_REPLICA, 1],
            'UpdateMetadata v4 (0.11)'     => [ApiKeys::UPDATE_METADATA, 4],
            'OffsetCommit v3 (0.11)'       => [ApiKeys::OFFSET_COMMIT, 3],
            'OffsetFetch v3 (0.11)'        => [ApiKeys::OFFSET_FETCH, 3],
            'GroupCoordinator v1 (0.11)'   => [ApiKeys::GROUP_COORDINATOR, 1],
            'JoinGroup v2 (0.11)'          => [ApiKeys::JOIN_GROUP, 2],
            'Heartbeat v1 (0.11)'          => [ApiKeys::HEARTBEAT, 1],
            'LeaveGroup v1 (0.11)'         => [ApiKeys::LEAVE_GROUP, 1],
            'SyncGroup v1 (0.11)'          => [ApiKeys::SYNC_GROUP, 1],
            'DescribeGroups v1 (0.11)'     => [ApiKeys::DESCRIBE_GROUPS, 1],
            'ListGroups v1 (0.11)'         => [ApiKeys::LIST_GROUPS, 1],
            'SaslHandshake v1 (0.11)'      => [ApiKeys::SASL_HANDSHAKE, 1],
            'CreateTopics v2 (0.11)'       => [ApiKeys::CREATE_TOPICS, 2],
            'DeleteTopics v1 (0.11)'       => [ApiKeys::DELETE_TOPICS, 1],
            'DeleteRecords (21, 0.11)'     => [21, 0],
            'InitProducerId (22, 0.11)'    => [22, 0],
        ];
    }

    #[DataProvider('unservedApiProvider')]
    public function testTheBrokerClosesTheConnectionForAnApiItDoesNotServe(int $apiKey, int $apiVersion): void
    {
        $result = $this->probe($apiKey, $apiVersion, 2000 + $apiKey * 10 + $apiVersion);

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "The broker did not close the connection for the api key {$apiKey} version {$apiVersion}, which Kafka "
            . '0.10.2.2 does not serve'
        );
        self::assertNull($result['correlationId'], 'a closed connection carries no response frame');
    }

    /**
     * ApiVersions is the one api whose unknown version is answered instead of costing the connection
     *
     * `RequestChannel.Request` @ 0.10.2.2 builds a dummy ApiVersions request for a version it cannot parse, so that
     * `KafkaApis.handleApiVersionsRequest` can answer `ApiVersionsResponse.fromError(UNSUPPORTED_VERSION)` - the
     * error code 35 with an empty api array, written in the **version 0** layout.
     */
    public function testApiVersionsAnswersAnUnknownVersionWithTheErrorCode35(): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::API_VERSIONS, 7, '', 3001);
        // The connection survives it, which is what makes the answer usable at all
        $served = $probe->send(ApiKeys::API_VERSIONS, 0, '', 3002);
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(3001, $result['correlationId']);
        self::assertSame(KafkaException::UNSUPPORTED_VERSION, self::errorCodeOf($result['body']));
        self::assertSame(
            6,
            strlen($result['body']),
            'the error answer is an int16 error code and an empty api array'
        );

        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(3002, $served['correlationId']);
    }

    /**
     * ControlledShutdown is the last api a 0.10.2.2 broker parses with its Scala class, and it ignores the version
     *
     * `RequestChannel.Request` calls `ControlledShutdownRequest.readFrom()` for key 7 before the request header is
     * even parsed - "this will be removed once we remove support for v0 of ControlledShutdownRequest", says the
     * source - and that parser only asks whether the version is above 0, to decide whether a client id follows. The
     * frame of the retired version 0 is therefore still answered, and so is a version above the maximum, although
     * ApiVersions reports the single version 1 for the api.
     *
     * @return array<string, array{int, bool}>
     */
    public static function controlledShutdownVersionProvider(): array
    {
        return [
            'v0, the header without a client id' => [0, false],
            'v1, the version of the table'        => [1, true],
            'v2, above the table'                 => [2, true],
        ];
    }

    #[DataProvider('controlledShutdownVersionProvider')]
    public function testControlledShutdownAnswersEveryVersionItIsSent(int $apiVersion, bool $withClientId): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(
            ApiKeys::CONTROLLED_SHUTDOWN,
            $apiVersion,
            RawApiProbe::int32(self::UNKNOWN_BROKER_ID),
            4000 + $apiVersion,
            $withClientId
        );
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(
            KafkaException::BROKER_NOT_AVAILABLE,
            self::errorCodeOf($result['body']),
            'the controller does not know the broker id, and says so for every version'
        );
    }

    /**
     * A body that does not match the schema of a served version costs the connection just as an unknown version does
     *
     * This is the `SchemaException` half of `processCompletedReceives`: the api key and the version are fine, the
     * bytes behind them are not. A JoinGroup v1 frame carrying a v0 body - the `rebalance_timeout` int32 is missing,
     * so the parser reads the protocol type as a member id of 25455 bytes - is the shortest way to produce one.
     */
    public function testAMalformedBodyOfAServedVersionAlsoClosesTheConnection(): void
    {
        $versionZeroBody = RawApiProbe::string($this->groupId)
            . RawApiProbe::int32(1000)
            . RawApiProbe::string('')
            . RawApiProbe::string('consumer')
            . RawApiProbe::int32(0);

        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::JOIN_GROUP, 1, $versionZeroBody, 5001);
        $probe->close();

        self::assertSame(RawApiProbe::CLOSED, $result['status']);
    }

    /**
     * A closed connection costs nothing but that connection: the next one is answered normally
     */
    public function testAClosedConnectionDoesNotAffectTheNextOne(): void
    {
        $closed = $this->probe(21, 0, 6001);
        $served = $this->probe(ApiKeys::METADATA, 0, 6002);

        self::assertSame(RawApiProbe::CLOSED, $closed['status'], 'DeleteRecords (21) is a Kafka 0.11 api key');
        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(6002, $served['correlationId']);
    }

    /**
     * The response of a served api grows with its version, which is what makes the version worth sending
     */
    public function testTheAnswerOfAnEmptyRequestGrowsWithTheVersionOfProduceAndFetch(): void
    {
        $emptyProduceV0 = $this->probe(ApiKeys::PRODUCE, 0, 7000)['body'];
        $emptyProduceV1 = $this->probe(ApiKeys::PRODUCE, 1, 7001)['body'];
        $emptyProduceV2 = $this->probe(ApiKeys::PRODUCE, 2, 7002)['body'];
        $emptyFetchV0   = $this->probe(ApiKeys::FETCH, 0, 7003)['body'];
        $emptyFetchV1   = $this->probe(ApiKeys::FETCH, 1, 7004)['body'];
        $emptyFetchV2   = $this->probe(ApiKeys::FETCH, 2, 7005)['body'];

        // An empty topic array is four zero bytes, the throttle time of an unthrottled client four more. The
        // `log_append_time` that Produce v2 adds sits inside a partition, so an empty answer does not show it, and
        // Fetch v2 only announces that the client understands message format v1 - its frame is the one of v1.
        self::assertSame(4, strlen($emptyProduceV0));
        self::assertSame(8, strlen($emptyProduceV1));
        self::assertSame(8, strlen($emptyProduceV2));
        self::assertSame(4, strlen($emptyFetchV0));
        self::assertSame(8, strlen($emptyFetchV1));
        self::assertSame(8, strlen($emptyFetchV2));
    }

    /**
     * The group apis of a 0.10.2.2 broker report the error codes they reported in 0.9
     */
    public function testGroupApisReportTheErrorCodesOfTheGroupProtocol(): void
    {
        $joinGroup = $this->probe(ApiKeys::JOIN_GROUP, 0, 8001);
        $heartbeat = $this->probe(ApiKeys::HEARTBEAT, 0, 8002);

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
     * SaslHandshake, the other api Kafka 0.10.0 added, is answered on the plaintext listener with 34
     *
     * On a SASL listener the request never reaches the api layer: `SaslServerAuthenticator` intercepts it while the
     * connection is still unauthenticated and answers it there. Everything that does get through to
     * `KafkaApis.handleSaslHandshakeRequest` @ 0.10.2.2 is therefore a handshake that arrived at the wrong moment,
     * and that method answers a constant **34** (IllegalSaslState) with the mechanisms the broker has enabled - no
     * matter which mechanism was asked for. The handshake itself belongs to the SASL listeners; this only pins that
     * the key is served on every listener.
     */
    public function testSaslHandshakeOnThePlaintextListenerIsAnsweredWithIllegalSaslState(): void
    {
        $result = $this->probe(ApiKeys::SASL_HANDSHAKE, 0, 9001);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(KafkaException::ILLEGAL_SASL_STATE, self::errorCodeOf($result['body']));
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
     * Builds the smallest well-formed body of the given api version that changes nothing on the cluster
     */
    private function body(int $apiKey, int $apiVersion): string
    {
        $emptyArray   = RawApiProbe::int32(0);
        $staleControl = RawApiProbe::int32(-1) . RawApiProbe::int32(-1);

        return match ($apiKey) {
            // RequiredAcks 1, a timeout and no topic at all
            ApiKeys::PRODUCE  => pack('n', 1) . RawApiProbe::int32(1000) . $emptyArray,
            // ReplicaId -1 (an ordinary consumer), MaxWaitTime, MinBytes, the MaxBytes of v3, and no topic
            ApiKeys::FETCH    => RawApiProbe::int32(-1) . RawApiProbe::int32(100) . RawApiProbe::int32(0)
                . ($apiVersion >= 3 ? RawApiProbe::int32(1048576) : '') . $emptyArray,
            ApiKeys::OFFSETS  => RawApiProbe::int32(-1) . $emptyArray,
            // v0 reads an empty array as "every topic", v1 and v2 as "no topic"; both are well formed
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
            // A session timeout of 1 ms is below group.min.session.timeout.ms, so no group is created; v1 carries
            // the rebalance timeout of `max.poll.interval.ms` between the session timeout and the member id
            ApiKeys::JOIN_GROUP        => RawApiProbe::string($this->groupId) . RawApiProbe::int32(1)
                . ($apiVersion >= 1 ? RawApiProbe::int32(1) : '')
                . RawApiProbe::string('') . RawApiProbe::string('consumer') . $emptyArray,
            ApiKeys::HEARTBEAT         => RawApiProbe::string($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::string('probe-member'),
            ApiKeys::LEAVE_GROUP       => RawApiProbe::string($this->groupId) . RawApiProbe::string('probe-member'),
            ApiKeys::SYNC_GROUP        => RawApiProbe::string($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::string('probe-member') . $emptyArray,
            ApiKeys::DESCRIBE_GROUPS   => $emptyArray,
            ApiKeys::LIST_GROUPS       => '',
            // A mechanism the listener does not offer, so the broker answers 33 and creates nothing
            ApiKeys::SASL_HANDSHAKE    => RawApiProbe::string('KAFKA-CLIENT-PROBE'),
            ApiKeys::API_VERSIONS      => '',
            // No topic to create and no topic to delete; v1 of CreateTopics ends with `validate_only`
            ApiKeys::CREATE_TOPICS     => $emptyArray . RawApiProbe::int32(1000)
                . ($apiVersion >= 1 ? pack('C', 1) : ''),
            ApiKeys::DELETE_TOPICS     => $emptyArray . RawApiProbe::int32(1000),
            default                    => '',
        };
    }

    /**
     * Reads the error code that an api puts in front of its response body
     */
    private static function errorCodeOf(string $body): int
    {
        self::assertGreaterThanOrEqual(2, strlen($body), 'the response carries no error code');

        return (int) unpack('s', strrev(substr($body, 0, 2)))[1];
    }

    private function client(): Client
    {
        return new Client($this->cluster(), $this->configuration());
    }

    private function admin(): AdminClient
    {
        return new AdminClient($this->cluster(), $this->configuration());
    }

    /**
     * Returns one broker of the cluster; every broker answers ApiVersions for itself
     */
    private function anyNode(): \Protocol\Kafka\Common\Node
    {
        $nodes = $this->cluster()->nodes();
        self::assertNotEmpty($nodes, 'The readiness probe must have resolved the cluster before the tests run');

        return reset($nodes);
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 'kafka-client-t1',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
        ] + ClientConfig::getDefaultConfiguration();
    }
}
