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
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV0;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;

/**
 * Establishes which api keys and versions a real Kafka 0.11.0.3 broker serves.
 *
 * Kafka 0.10.0 added the api that answers that question - **ApiVersions**, key 18 - so this class no longer has to
 * guess it the way the `0.8.x` and `0.9.x` lines did. The first half of the suite asks the broker with
 * {@see Client::apiVersions()} and pins its answer, which is the api-key table of `docs/protocol/0.11.0.md`.
 *
 * The second half is still a raw probe ({@see RawApiProbe}), because the *edges* of that table are not in it: what
 * the broker does with a key or a version it does not serve is behaviour, not data. Kafka 0.10 changed that
 * behaviour and 0.11 kept it, and this is where it is verified:
 *
 * * A 0.9.0.1 broker **dropped** a frame it could not parse and kept the connection open, so a client waited for its
 *   own timeout ({@see RawApiProbe::SILENT}).
 * * A 0.11.0.3 broker **closes the connection** ({@see RawApiProbe::CLOSED}). `SocketServer.processCompletedReceives`
 *   @ 0.11.0.3 catches the `InvalidRequestException` that `RequestChannel.Request` throws for an api key or version
 *   `AbstractRequest.getRequest()` does not know, and the `SchemaException` of a body that does not match the
 *   schema, and calls `close()` on the channel; `docker logs kafka-0-11-0-3` shows
 *   `ERROR Closing socket for ... because of error` with the reason.
 *
 * Two apis are exceptions to that rule and both are verified here: ApiVersions itself, which answers an unknown
 * version with the error code 35, and ControlledShutdown, the last api that a 0.11.0.3 broker still parses with the
 * Scala class of the 0.8 line, which answers every version.
 *
 * The probe never changes the state of the cluster: the broker-to-broker apis are sent with the stale controller
 * epoch -1 and empty partition sets, ControlledShutdown asks for a broker id that does not exist, the group and
 * transaction apis use a group id and a transactional id that no other test uses, and every api that takes a list of
 * things to change - CreateTopics, DeleteTopics, DeleteRecords, WriteTxnMarkers, AlterConfigs - is sent with an
 * empty array.
 *
 * @see docs/protocol/0.11.0.md, section "API keys"
 */
#[CoversClass(ApiKeys::class)]
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsRequestV0::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseV0::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
final class ApiVersionProbeTest extends IntegrationTestCase
{
    /**
     * An unknown broker id, so that ControlledShutdown reports an error instead of stopping a broker
     */
    private const int UNKNOWN_BROKER_ID = 4242;

    /**
     * The api table of Kafka 0.11.0.3, as `api key => [minimum version, maximum version]`
     *
     * This is what `Protocol.CURR_VERSION` @ 0.11.0.3 declares and what the container really answers. Key 7 is the
     * only entry whose minimum is not 0: ControlledShutdown v0 uses a request header without a client id, which the
     * Java client cannot build any more.
     *
     * The set itself is not a constant of the release: `ApiVersionsResponse.createApiVersionsResponse()` drops every
     * api whose `minRequiredInterBrokerMagic` is above the message format the broker runs with, so a 0.11 broker
     * configured with `inter.broker.protocol.version=0.10.2` reports fewer keys than this. The container runs the
     * default 0.11.0 format, which is where these 34 keys come from.
     */
    private const array SERVED_APIS = [
        ApiKeys::PRODUCE                 => [0, 3],
        ApiKeys::FETCH                   => [0, 5],
        ApiKeys::OFFSETS                 => [0, 2],
        ApiKeys::METADATA                => [0, 4],
        ApiKeys::LEADER_AND_ISR          => [0, 0],
        ApiKeys::STOP_REPLICA            => [0, 0],
        ApiKeys::UPDATE_METADATA         => [0, 3],
        ApiKeys::CONTROLLED_SHUTDOWN     => [1, 1],
        ApiKeys::OFFSET_COMMIT           => [0, 3],
        ApiKeys::OFFSET_FETCH            => [0, 3],
        ApiKeys::GROUP_COORDINATOR       => [0, 1],
        ApiKeys::JOIN_GROUP              => [0, 2],
        ApiKeys::HEARTBEAT               => [0, 1],
        ApiKeys::LEAVE_GROUP             => [0, 1],
        ApiKeys::SYNC_GROUP              => [0, 1],
        ApiKeys::DESCRIBE_GROUPS         => [0, 1],
        ApiKeys::LIST_GROUPS             => [0, 1],
        ApiKeys::SASL_HANDSHAKE          => [0, 0],
        ApiKeys::API_VERSIONS            => [0, 1],
        ApiKeys::CREATE_TOPICS           => [0, 2],
        ApiKeys::DELETE_TOPICS           => [0, 1],
        ApiKeys::DELETE_RECORDS          => [0, 0],
        ApiKeys::INIT_PRODUCER_ID        => [0, 0],
        ApiKeys::OFFSET_FOR_LEADER_EPOCH => [0, 0],
        ApiKeys::ADD_PARTITIONS_TO_TXN   => [0, 0],
        ApiKeys::ADD_OFFSETS_TO_TXN      => [0, 0],
        ApiKeys::END_TXN                 => [0, 0],
        ApiKeys::WRITE_TXN_MARKERS       => [0, 0],
        ApiKeys::TXN_OFFSET_COMMIT       => [0, 0],
        ApiKeys::DESCRIBE_ACLS           => [0, 0],
        ApiKeys::CREATE_ACLS             => [0, 0],
        ApiKeys::DELETE_ACLS             => [0, 0],
        ApiKeys::DESCRIBE_CONFIGS        => [0, 0],
        ApiKeys::ALTER_CONFIGS           => [0, 0],
    ];

    /**
     * The first api key above the table; `AlterReplicaLogDirs` is Kafka 1.0 and no 0.11.0.3 broker knows it
     */
    private const int UNKNOWN_API_KEY = 34;

    /**
     * Cluster of this test class, resolved once
     */
    private static ?Cluster $sharedCluster = null;

    /**
     * Group id of this test class; every group request of the probe fails before a group is created
     */
    private string $groupId;

    /**
     * Transactional id of this test class; no producer of any test ever uses it
     */
    private string $transactionalId;

    protected function setUp(): void
    {
        parent::setUp();

        $suffix                = bin2hex(random_bytes(6));
        $this->groupId         = 't1-probe-' . $suffix;
        $this->transactionalId = 't1-probe-txn-' . $suffix;
    }

    public function testTheBrokerReportsEveryApiOfKafka01103(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);

        $reported = [];
        foreach ($response->apiVersions as $apiKey => $metadata) {
            $reported[$apiKey] = [$metadata->minVersion, $metadata->maxVersion];
        }

        self::assertSame(self::SERVED_APIS, $reported, 'the api table of the protocol document');
    }

    /**
     * The client sends version 1, so the answer carries the throttle time that Kafka 0.11 appended to it
     */
    public function testTheAnswerOfVersionOneCarriesTheTrailingThrottleTime(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertSame(1, ApiVersionsRequest::VERSION, 'this line sends ApiVersions v1');
        self::assertSame(0, ApiVersionsRequestV0::VERSION);
        self::assertSame(0, $response->throttleTimeMs, 'an ApiVersions request is never throttled without a quota');

        // The v1 frame is the v0 frame plus the four bytes of the throttle time - the one api of Kafka 0.11 that
        // appends the field instead of prepending it, so that the leading error code stays where a v0 client
        // expects it
        $versionZero = $this->probe(ApiKeys::API_VERSIONS, 0, 3010)['body'];
        $versionOne  = $this->probe(ApiKeys::API_VERSIONS, 1, 3011)['body'];

        self::assertSame(strlen($versionZero) + 4, strlen($versionOne));
        self::assertSame($versionZero, substr($versionOne, 0, -4));
        self::assertSame(0, (int) unpack('N', substr($versionOne, -4))[1], 'the trailing throttle_time_ms');
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

        self::assertTrue($response->supports(ApiKeys::FETCH, 5), 'Fetch v5 arrived with Kafka 0.11');
        self::assertFalse($response->supports(ApiKeys::FETCH, 6), 'Fetch v6 is Kafka 1.0');
        self::assertFalse($response->supports(ApiKeys::CONTROLLED_SHUTDOWN, 0), 'the minimum version is inclusive');
        self::assertSame(4, $response->maxVersionOf(ApiKeys::METADATA));
        self::assertSame(0, $response->maxVersionOf(ApiKeys::DELETE_RECORDS), 'DeleteRecords (21) is Kafka 0.11');
        self::assertNull(
            $response->maxVersionOf(self::UNKNOWN_API_KEY),
            'AlterReplicaLogDirs (34) is Kafka 1.0 and is not reported at all'
        );
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
     * The two exceptions of the table are not here: ControlledShutdown (7) answers every version and ApiVersions
     * (18) answers an unknown version with the error code 35; both have a test of their own below.
     *
     * @return array<string, array{int, int}>
     */
    public static function unservedApiProvider(): array
    {
        $cases = [];
        foreach (self::SERVED_APIS as $apiKey => [, $maxVersion]) {
            if ($apiKey === ApiKeys::CONTROLLED_SHUTDOWN || $apiKey === ApiKeys::API_VERSIONS) {
                continue;
            }
            $cases["key {$apiKey} v" . ($maxVersion + 1)] = [$apiKey, $maxVersion + 1];
        }
        $cases['key ' . self::UNKNOWN_API_KEY . ' v0 (Kafka 1.0)'] = [self::UNKNOWN_API_KEY, 0];

        return $cases;
    }

    #[DataProvider('unservedApiProvider')]
    public function testTheBrokerClosesTheConnectionForAnApiItDoesNotServe(int $apiKey, int $apiVersion): void
    {
        $result = $this->probe($apiKey, $apiVersion, 2000 + $apiKey * 10 + $apiVersion);

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "The broker did not close the connection for the api key {$apiKey} version {$apiVersion}, which Kafka "
            . '0.11.0.3 does not serve'
        );
        self::assertNull($result['correlationId'], 'a closed connection carries no response frame');
    }

    /**
     * ApiVersions is the one api whose unknown version is answered instead of costing the connection
     *
     * `RequestChannel.Request` @ 0.11.0.3 builds a dummy ApiVersions request for a version it cannot parse, so that
     * `KafkaApis.handleApiVersionsRequest` can answer `ApiVersionsResponse.unsupportedVersionSend()` - the error
     * code 35 with an empty api array, written in the **version 0** layout, i.e. without the throttle time that the
     * version 1 answer of the same broker carries.
     */
    public function testApiVersionsAnswersAnUnknownVersionWithTheErrorCode35(): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::API_VERSIONS, 7, '', 3001);
        // The connection survives it, which is what makes the answer usable at all
        $served = $probe->send(ApiKeys::API_VERSIONS, 1, '', 3002);
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(3001, $result['correlationId']);
        self::assertSame(KafkaException::UNSUPPORTED_VERSION, self::errorCodeOf($result['body']));
        self::assertSame(
            6,
            strlen($result['body']),
            'the error answer is an int16 error code and an empty api array, in the version 0 layout'
        );

        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(3002, $served['correlationId']);
    }

    /**
     * ControlledShutdown is the last api a 0.11.0.3 broker parses with its Scala class, and it ignores the version
     *
     * `RequestChannel.Request` calls `ControlledShutdownRequest.readFrom()` for key 7 before the request header is
     * even parsed - "this will be removed once we remove support for v0 of ControlledShutdownRequest", says the
     * source, still, in 0.11 - and that parser only asks whether the version is above 0, to decide whether a client
     * id follows. The frame of the retired version 0 is therefore still answered, and so is a version above the
     * maximum, although ApiVersions reports the single version 1 for the api.
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
        $closed = $this->probe(self::UNKNOWN_API_KEY, 0, 6001);
        $served = $this->probe(ApiKeys::METADATA, 4, 6002);

        self::assertSame(
            RawApiProbe::CLOSED,
            $closed['status'],
            'AlterReplicaLogDirs (34) is a Kafka 1.0 api key'
        );
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
        $emptyProduceV3 = $this->probe(ApiKeys::PRODUCE, 3, 7003)['body'];
        $emptyFetchV0   = $this->probe(ApiKeys::FETCH, 0, 7004)['body'];
        $emptyFetchV1   = $this->probe(ApiKeys::FETCH, 1, 7005)['body'];
        $emptyFetchV5   = $this->probe(ApiKeys::FETCH, 5, 7006)['body'];

        // An empty topic array is four zero bytes, the throttle time of an unthrottled client four more. Everything
        // Kafka 0.11 adds to these two apis - `transactional_id`, `log_start_offset`, `isolation_level`,
        // `last_stable_offset`, the aborted transactions - sits inside a partition entry, so an answer without a
        // single topic is the same eight bytes it was in 0.10.
        self::assertSame(4, strlen($emptyProduceV0));
        self::assertSame(8, strlen($emptyProduceV1));
        self::assertSame(8, strlen($emptyProduceV3));
        self::assertSame(4, strlen($emptyFetchV0));
        self::assertSame(8, strlen($emptyFetchV1));
        self::assertSame(8, strlen($emptyFetchV5));
    }

    /**
     * The group apis of a 0.11.0.3 broker report the error codes they reported in 0.9, behind a throttle time
     *
     * Every one of them gained a version with Kafka 0.11 whose only change is the leading `throttle_time_ms` int32
     * of the answer (KIP-124), so the error code of the new version sits four bytes further in than it did.
     */
    public function testGroupApisReportTheErrorCodesOfTheGroupProtocol(): void
    {
        $joinGroupV0 = $this->probe(ApiKeys::JOIN_GROUP, 0, 8001);
        $heartbeatV0 = $this->probe(ApiKeys::HEARTBEAT, 0, 8002);
        $joinGroupV2 = $this->probe(ApiKeys::JOIN_GROUP, 2, 8003);
        $heartbeatV1 = $this->probe(ApiKeys::HEARTBEAT, 1, 8004);

        // The probe joins with a session timeout of 1 ms, which is below group.min.session.timeout.ms
        $joinGroupCodes = [
            KafkaException::INVALID_SESSION_TIMEOUT,
            KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
            KafkaException::GROUP_LOAD_IN_PROGRESS,
        ];
        // Heartbeat of a member of a group that does not exist
        $heartbeatCodes = [
            KafkaException::UNKNOWN_MEMBER_ID,
            KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
            KafkaException::GROUP_LOAD_IN_PROGRESS,
        ];

        self::assertContains(self::errorCodeOf($joinGroupV0['body']), $joinGroupCodes);
        self::assertContains(self::errorCodeOf($heartbeatV0['body']), $heartbeatCodes);
        self::assertContains(self::errorCodeBehindTheThrottleTimeOf($joinGroupV2['body']), $joinGroupCodes);
        self::assertContains(self::errorCodeBehindTheThrottleTimeOf($heartbeatV1['body']), $heartbeatCodes);
        self::assertSame(0, self::throttleTimeOf($joinGroupV2['body']), 'no quota, no throttling');
        self::assertSame(0, self::throttleTimeOf($heartbeatV1['body']));
    }

    /**
     * The ACL apis (29, 30, 31) are served, and answer 54 while the broker has no authorizer
     *
     * This is why the epic left them out of the line: `KafkaApis.handleDescribeAcls` @ 0.11.0.3 answers
     * `Errors.SECURITY_DISABLED` for every one of them unless `authorizer.class.name` is configured, so no wire
     * vector of a real answer can be captured on the shared container. The keys themselves are in `ApiKeys` and in
     * the api-key table, because the broker reports them.
     *
     * @return array<string, array{int}>
     */
    public static function aclApiProvider(): array
    {
        return [
            'DescribeAcls (29)' => [ApiKeys::DESCRIBE_ACLS],
            'CreateAcls (30)'   => [ApiKeys::CREATE_ACLS],
            'DeleteAcls (31)'   => [ApiKeys::DELETE_ACLS],
        ];
    }

    #[DataProvider('aclApiProvider')]
    public function testTheAclApisAreServedButDisabledWithoutAnAuthorizer(int $apiKey): void
    {
        $result = $this->probe($apiKey, 0, 9100 + $apiKey);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(0, self::throttleTimeOf($result['body']));
        // DescribeAcls answers one top-level error, CreateAcls and DeleteAcls one per entry - and the probe sends
        // an empty entry list to those two, so only the filter of DescribeAcls produces a code here
        if ($apiKey === ApiKeys::DESCRIBE_ACLS) {
            self::assertSame(
                KafkaException::SECURITY_DISABLED,
                self::errorCodeBehindTheThrottleTimeOf($result['body']),
                'the container runs without an authorizer.class.name'
            );
        }
    }

    /**
     * SaslHandshake, the other api Kafka 0.10.0 added, is answered on the plaintext listener with 34
     *
     * On a SASL listener the request never reaches the api layer: `SaslServerAuthenticator` intercepts it while the
     * connection is still unauthenticated and answers it there. Everything that does get through to
     * `KafkaApis.handleSaslHandshakeRequest` @ 0.11.0.3 is therefore a handshake that arrived at the wrong moment,
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
        $nullString   = pack('n', -1);
        $staleControl = RawApiProbe::int32(-1) . RawApiProbe::int32(-1);
        // No producer of the cluster owns this id, so every transaction api answers an error and changes nothing
        $unknownProducer = RawApiProbe::int64(-1) . pack('n', -1);

        return match ($apiKey) {
            // v3 opens with the transactional id of KIP-98, then RequiredAcks 1, a timeout and no topic at all
            ApiKeys::PRODUCE  => ($apiVersion >= 3 ? $nullString : '') . pack('n', 1) . RawApiProbe::int32(1000)
                . $emptyArray,
            // ReplicaId -1 (an ordinary consumer), MaxWaitTime, MinBytes, the MaxBytes of v3, the isolation level
            // of v4 (0 = read_uncommitted), and no topic
            ApiKeys::FETCH    => RawApiProbe::int32(-1) . RawApiProbe::int32(100) . RawApiProbe::int32(0)
                . ($apiVersion >= 3 ? RawApiProbe::int32(1048576) : '')
                . ($apiVersion >= 4 ? pack('c', 0) : '') . $emptyArray,
            ApiKeys::OFFSETS  => RawApiProbe::int32(-1) . ($apiVersion >= 2 ? pack('c', 0) : '') . $emptyArray,
            // v0 reads an empty array as "every topic", v1 and above as "no topic"; v4 ends with
            // `allow_auto_topic_creation`, which is sent as false so that the probe creates nothing
            ApiKeys::METADATA => $emptyArray . ($apiVersion >= 4 ? pack('C', 0) : ''),
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
            ApiKeys::OFFSET_FETCH => RawApiProbe::string($this->groupId) . $emptyArray,
            // v1 renamed the field to `coordinator_key` and added the type; 0 is a group, 1 a transaction - the
            // probe asks for a group, so that the `__transaction_state` topic is not created behind its back
            ApiKeys::GROUP_COORDINATOR => RawApiProbe::string($this->groupId)
                . ($apiVersion >= 1 ? pack('c', 0) : ''),
            // A session timeout of 1 ms is below group.min.session.timeout.ms, so no group is created; v1 and v2
            // carry the rebalance timeout of `max.poll.interval.ms` between the session timeout and the member id
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
            // A mechanism the listener does not offer, so the broker answers 34 and creates nothing
            ApiKeys::SASL_HANDSHAKE    => RawApiProbe::string('KAFKA-CLIENT-PROBE'),
            ApiKeys::API_VERSIONS      => '',
            // No topic to create and no topic to delete; v1 and v2 of CreateTopics end with `validate_only`
            ApiKeys::CREATE_TOPICS     => $emptyArray . RawApiProbe::int32(1000)
                . ($apiVersion >= 1 ? pack('C', 1) : ''),
            ApiKeys::DELETE_TOPICS     => $emptyArray . RawApiProbe::int32(1000),
            // No log to truncate and no partition to look an epoch up in
            ApiKeys::DELETE_RECORDS          => $emptyArray . RawApiProbe::int32(1000),
            ApiKeys::OFFSET_FOR_LEADER_EPOCH => $emptyArray,
            // A null transactional id asks for a bare producer id, which allocates one and creates no transaction
            ApiKeys::INIT_PRODUCER_ID        => $nullString . RawApiProbe::int32(1000),
            // Every transaction api names a transactional id the coordinator has never seen, with the producer id
            // -1: the answer is an error, and no transaction state is written
            ApiKeys::ADD_PARTITIONS_TO_TXN   => RawApiProbe::string($this->transactionalId) . $unknownProducer
                . $emptyArray,
            ApiKeys::ADD_OFFSETS_TO_TXN      => RawApiProbe::string($this->transactionalId) . $unknownProducer
                . RawApiProbe::string($this->groupId),
            ApiKeys::END_TXN                 => RawApiProbe::string($this->transactionalId) . $unknownProducer
                . pack('C', 0),
            ApiKeys::WRITE_TXN_MARKERS       => $emptyArray,
            ApiKeys::TXN_OFFSET_COMMIT       => RawApiProbe::string($this->transactionalId)
                . RawApiProbe::string($this->groupId) . $unknownProducer . $emptyArray,
            // A filter that matches nothing, on a broker that has no authorizer at all
            ApiKeys::DESCRIBE_ACLS           => pack('c', 1) . $nullString . $nullString . $nullString
                . pack('c', 1) . pack('c', 1),
            ApiKeys::CREATE_ACLS             => $emptyArray,
            ApiKeys::DELETE_ACLS             => $emptyArray,
            // No resource to describe and none to change
            ApiKeys::DESCRIBE_CONFIGS        => $emptyArray,
            ApiKeys::ALTER_CONFIGS           => $emptyArray . pack('C', 1),
            default                          => '',
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

    /**
     * Reads the error code of an answer that opens with the `throttle_time_ms` int32 of Kafka 0.11
     */
    private static function errorCodeBehindTheThrottleTimeOf(string $body): int
    {
        self::assertGreaterThanOrEqual(6, strlen($body), 'the response carries no throttle time and error code');

        return (int) unpack('s', strrev(substr($body, 4, 2)))[1];
    }

    /**
     * Reads the leading `throttle_time_ms` int32 of an answer of Kafka 0.11
     */
    private static function throttleTimeOf(string $body): int
    {
        self::assertGreaterThanOrEqual(4, strlen($body), 'the response carries no throttle time');

        return (int) unpack('N', substr($body, 0, 4))[1];
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
