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
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV1;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV1;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;

/**
 * Establishes which api keys and versions a real Kafka 2.8.2 broker serves.
 *
 * Kafka 0.10.0 added the api that answers that question - **ApiVersions**, key 18 - so this class no longer has to
 * guess it the way the `0.8.x` and `0.9.x` lines did. The first half of the suite asks the broker with
 * {@see Client::apiVersions()} and pins its answer, which is the api-key table of `docs/protocol/2.8.md`: **56
 * keys**, 0 to 51 and 56, 57, 60, 61, the `zkBroker` listener set of the JSON message specifications @ 2.8.2.
 *
 * The second half is still a raw probe ({@see RawApiProbe}), because the *edges* of that table are not in it: what
 * the broker does with a key or a version it does not serve is behaviour, not data. Kafka 0.10 changed that
 * behaviour and every release since keeps it, and this is where it is verified:
 *
 * * A 0.9.0.1 broker **dropped** a frame it could not parse and kept the connection open, so a client waited for its
 *   own timeout ({@see RawApiProbe::SILENT}).
 * * A 2.8.2 broker **closes the connection** ({@see RawApiProbe::CLOSED}). `SocketServer.processCompletedReceives`
 *   @ 2.8.2 catches the `InvalidRequestException` that `RequestContext.parseRequest` throws for a version the
 *   generated message class refuses (`UnsupportedVersionException: The OFFSET_COMMIT protocol does not support
 *   version 9`), for an api key that is not enabled on the listener (`Received request api key VOTE which is not
 *   enabled`) and for a key no `ApiKeys` entry knows (`Unexpected api key: 65`), and calls `close()` on the
 *   channel; `docker logs kafka-2-8-2` shows `ERROR Closing socket for ... because of error` with the reason.
 *
 * **Exactly one api is an exception to that rule**: ApiVersions itself, which answers an unknown version with the
 * error code 35 on a connection that stays open - as long as the frame carries the request header that the
 * *requested* version prescribes, which since KIP-482 is the header v2 for every version above 2.
 *
 * The frame of every key is sent at the **maximum version the broker reports**, which for 34 of the 56 keys is a
 * **flexible** version (KIP-482): a compact body, the request header v2 and a tagged-field section at the end of
 * every structure. The bytes are built by the fixture, not by the schema engine, exactly as the frames above the
 * table are - the engine of this line learns the flexible encoding in the ticket that follows this one, and the
 * probe has to keep working without it.
 *
 * The probe never changes the state of the cluster: the broker-to-broker apis are sent with the stale controller
 * epoch -1 and empty partition sets, ControlledShutdown asks for a broker id that does not exist, the group and
 * transaction apis use a group id and a transactional id that no other test uses, and every api that takes a list of
 * things to change - CreateTopics, DeleteTopics, DeleteRecords, WriteTxnMarkers, AlterConfigs, ElectLeaders,
 * AlterPartitionReassignments, the quota and SCRAM apis - is sent with an empty array.
 *
 * @see docs/protocol/2.8.md, section "API keys"
 */
#[CoversClass(ApiKeys::class)]
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsRequestV0::class)]
#[CoversClass(ApiVersionsRequestV1::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseV0::class)]
#[CoversClass(ApiVersionsResponseV1::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
final class ApiVersionProbeTest extends IntegrationTestCase
{
    /**
     * An unknown broker id, so that ControlledShutdown reports an error instead of stopping a broker
     */
    private const int UNKNOWN_BROKER_ID = 4242;

    /**
     * The api table of Kafka 2.8.2, as `api key => [minimum version, maximum version]`
     *
     * This is what the `validVersions` of the JSON message specifications @ 2.8.2 declare and what the container
     * really answers. **Every minimum is 0** since Kafka 1.0, which gave the client-id-less header of
     * ControlledShutdown v0 a schema of its own.
     *
     * The set itself is not a constant of the release: from Kafka 2.8 (KIP-500) the answer is built from the apis
     * of the **listener** the request arrived on (`ApiVersionManager.apiVersionResponse` @ 2.8.2), so the KRaft apis
     * of the controller listener - 52 to 55, 58, 59 and 62 to 64, {@see self::UNSERVED_KEYS} - never appear on a
     * ZooKeeper-backed broker, and an api whose `minRequiredInterBrokerMagic` is above the message format of the
     * broker is dropped as it was before. The container runs `2.8-IV1`, which is where these 56 keys come from.
     */
    private const array SERVED_APIS = [
        ApiKeys::PRODUCE                        => [0, 9],
        ApiKeys::FETCH                          => [0, 12],
        ApiKeys::OFFSETS                        => [0, 6],
        ApiKeys::METADATA                       => [0, 11],
        ApiKeys::LEADER_AND_ISR                 => [0, 5],
        ApiKeys::STOP_REPLICA                   => [0, 3],
        ApiKeys::UPDATE_METADATA                => [0, 7],
        ApiKeys::CONTROLLED_SHUTDOWN            => [0, 3],
        ApiKeys::OFFSET_COMMIT                  => [0, 8],
        ApiKeys::OFFSET_FETCH                   => [0, 7],
        ApiKeys::GROUP_COORDINATOR              => [0, 3],
        ApiKeys::JOIN_GROUP                     => [0, 7],
        ApiKeys::HEARTBEAT                      => [0, 4],
        ApiKeys::LEAVE_GROUP                    => [0, 4],
        ApiKeys::SYNC_GROUP                     => [0, 5],
        ApiKeys::DESCRIBE_GROUPS                => [0, 5],
        ApiKeys::LIST_GROUPS                    => [0, 4],
        ApiKeys::SASL_HANDSHAKE                 => [0, 1],
        ApiKeys::API_VERSIONS                   => [0, 3],
        ApiKeys::CREATE_TOPICS                  => [0, 7],
        ApiKeys::DELETE_TOPICS                  => [0, 6],
        ApiKeys::DELETE_RECORDS                 => [0, 2],
        ApiKeys::INIT_PRODUCER_ID               => [0, 4],
        ApiKeys::OFFSET_FOR_LEADER_EPOCH        => [0, 4],
        ApiKeys::ADD_PARTITIONS_TO_TXN          => [0, 3],
        ApiKeys::ADD_OFFSETS_TO_TXN             => [0, 3],
        ApiKeys::END_TXN                        => [0, 3],
        ApiKeys::WRITE_TXN_MARKERS              => [0, 1],
        ApiKeys::TXN_OFFSET_COMMIT              => [0, 3],
        ApiKeys::DESCRIBE_ACLS                  => [0, 2],
        ApiKeys::CREATE_ACLS                    => [0, 2],
        ApiKeys::DELETE_ACLS                    => [0, 2],
        ApiKeys::DESCRIBE_CONFIGS               => [0, 4],
        ApiKeys::ALTER_CONFIGS                  => [0, 2],
        ApiKeys::ALTER_REPLICA_LOG_DIRS         => [0, 2],
        ApiKeys::DESCRIBE_LOG_DIRS              => [0, 2],
        ApiKeys::SASL_AUTHENTICATE              => [0, 2],
        ApiKeys::CREATE_PARTITIONS              => [0, 3],
        ApiKeys::CREATE_DELEGATION_TOKEN        => [0, 2],
        ApiKeys::RENEW_DELEGATION_TOKEN         => [0, 2],
        ApiKeys::EXPIRE_DELEGATION_TOKEN        => [0, 2],
        ApiKeys::DESCRIBE_DELEGATION_TOKEN      => [0, 2],
        ApiKeys::DELETE_GROUPS                  => [0, 2],
        ApiKeys::ELECT_LEADERS                  => [0, 2],
        ApiKeys::INCREMENTAL_ALTER_CONFIGS      => [0, 1],
        ApiKeys::ALTER_PARTITION_REASSIGNMENTS  => [0, 0],
        ApiKeys::LIST_PARTITION_REASSIGNMENTS   => [0, 0],
        ApiKeys::OFFSET_DELETE                  => [0, 0],
        ApiKeys::DESCRIBE_CLIENT_QUOTAS         => [0, 1],
        ApiKeys::ALTER_CLIENT_QUOTAS            => [0, 1],
        ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS => [0, 0],
        ApiKeys::ALTER_USER_SCRAM_CREDENTIALS   => [0, 0],
        ApiKeys::ALTER_ISR                      => [0, 0],
        ApiKeys::UPDATE_FEATURES                => [0, 0],
        ApiKeys::DESCRIBE_CLUSTER               => [0, 0],
        ApiKeys::DESCRIBE_PRODUCERS             => [0, 0],
    ];

    /**
     * The first **flexible** version of every api that has one, as `api key => version` (KIP-482, Kafka 2.4)
     *
     * The `flexibleVersions` of the JSON message specification of the request @ 2.8.2, verified frame by frame
     * against the container: a version below this one is refused when it is sent with a compact body, and a version
     * from it on is refused when it is not. Only two apis of the table never became flexible - SaslHandshake (17),
     * which was frozen when SaslAuthenticate took over the token exchange, and OffsetDelete (47), the one api of
     * Kafka 2.4 that was written without it - and they are simply absent here.
     *
     * The *request* and the *response* of an api can be flexible at different versions (Metadata is flexible from
     * v9, its response from v9 as well, but JoinGroup v6 and OffsetFetch v6 are flexible while their v5 is not),
     * so this table is the one of the request; the answer of the container tells the rest.
     */
    private const array FLEXIBLE_FROM = [
        ApiKeys::PRODUCE                        => 9,
        ApiKeys::FETCH                          => 12,
        ApiKeys::OFFSETS                        => 6,
        ApiKeys::METADATA                       => 9,
        ApiKeys::LEADER_AND_ISR                 => 4,
        ApiKeys::STOP_REPLICA                   => 2,
        ApiKeys::UPDATE_METADATA                => 6,
        ApiKeys::CONTROLLED_SHUTDOWN            => 3,
        ApiKeys::OFFSET_COMMIT                  => 8,
        ApiKeys::OFFSET_FETCH                   => 6,
        ApiKeys::GROUP_COORDINATOR              => 3,
        ApiKeys::JOIN_GROUP                     => 6,
        ApiKeys::HEARTBEAT                      => 4,
        ApiKeys::LEAVE_GROUP                    => 4,
        ApiKeys::SYNC_GROUP                     => 4,
        ApiKeys::DESCRIBE_GROUPS                => 5,
        ApiKeys::LIST_GROUPS                    => 3,
        ApiKeys::API_VERSIONS                   => 3,
        ApiKeys::CREATE_TOPICS                  => 5,
        ApiKeys::DELETE_TOPICS                  => 4,
        ApiKeys::DELETE_RECORDS                 => 2,
        ApiKeys::INIT_PRODUCER_ID               => 2,
        ApiKeys::OFFSET_FOR_LEADER_EPOCH        => 4,
        ApiKeys::ADD_PARTITIONS_TO_TXN          => 3,
        ApiKeys::ADD_OFFSETS_TO_TXN             => 3,
        ApiKeys::END_TXN                        => 3,
        ApiKeys::WRITE_TXN_MARKERS              => 1,
        ApiKeys::TXN_OFFSET_COMMIT              => 3,
        ApiKeys::DESCRIBE_ACLS                  => 2,
        ApiKeys::CREATE_ACLS                    => 2,
        ApiKeys::DELETE_ACLS                    => 2,
        ApiKeys::DESCRIBE_CONFIGS               => 4,
        ApiKeys::ALTER_CONFIGS                  => 2,
        ApiKeys::ALTER_REPLICA_LOG_DIRS         => 2,
        ApiKeys::DESCRIBE_LOG_DIRS              => 2,
        ApiKeys::SASL_AUTHENTICATE              => 2,
        ApiKeys::CREATE_PARTITIONS              => 2,
        ApiKeys::CREATE_DELEGATION_TOKEN        => 2,
        ApiKeys::RENEW_DELEGATION_TOKEN         => 2,
        ApiKeys::EXPIRE_DELEGATION_TOKEN        => 2,
        ApiKeys::DESCRIBE_DELEGATION_TOKEN      => 2,
        ApiKeys::DELETE_GROUPS                  => 2,
        ApiKeys::ELECT_LEADERS                  => 2,
        ApiKeys::INCREMENTAL_ALTER_CONFIGS      => 1,
        ApiKeys::ALTER_PARTITION_REASSIGNMENTS  => 0,
        ApiKeys::LIST_PARTITION_REASSIGNMENTS   => 0,
        ApiKeys::DESCRIBE_CLIENT_QUOTAS         => 1,
        ApiKeys::ALTER_CLIENT_QUOTAS            => 1,
        ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS => 0,
        ApiKeys::ALTER_USER_SCRAM_CREDENTIALS   => 0,
        ApiKeys::ALTER_ISR                      => 0,
        ApiKeys::UPDATE_FEATURES                => 0,
        ApiKeys::DESCRIBE_CLUSTER               => 0,
        ApiKeys::DESCRIBE_PRODUCERS             => 0,
    ];

    /**
     * The api keys of `ApiKeys.java` @ 2.8.2 that a ZooKeeper-backed broker does not serve at all
     *
     * The raft apis of KIP-595 (52 to 55), the `Envelope` of KIP-590 (58), `FetchSnapshot` (59) and the broker
     * registration apis of KIP-631 (62 to 64) belong to the **controller** listener of a KRaft cluster. They are
     * part of the protocol of Kafka 2.8 - their constants exist in `ApiKeys` so that any frame of the release can be
     * named - but the container is ZooKeeper-backed, so they are missing from its ApiVersions answer and a frame of
     * one of them costs the connection: `Received request api key VOTE which is not enabled`.
     */
    private const array UNSERVED_KEYS = [
        ApiKeys::VOTE,
        ApiKeys::BEGIN_QUORUM_EPOCH,
        ApiKeys::END_QUORUM_EPOCH,
        ApiKeys::DESCRIBE_QUORUM,
        ApiKeys::ENVELOPE,
        ApiKeys::FETCH_SNAPSHOT,
        ApiKeys::BROKER_REGISTRATION,
        ApiKeys::BROKER_HEARTBEAT,
        ApiKeys::UNREGISTER_BROKER,
    ];

    /**
     * The first api key above the table of `ApiKeys.java` @ 2.8.2, which ends at `UnregisterBroker` (64)
     *
     * `DescribeTransactions` (65) and `ListTransactions` (66) are Kafka 3.0, and no 2.8.2 broker can even name them:
     * `ApiKeys.forId(65)` throws, and the broker closes the connection while it parses the request header.
     */
    private const int UNKNOWN_API_KEY = 65;

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

    public function testTheBrokerReportsEveryApiOfKafka282(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);

        $reported = [];
        foreach ($response->apiVersions as $apiKey => $metadata) {
            $reported[$apiKey] = [$metadata->minVersion, $metadata->maxVersion];
        }

        self::assertSame(self::SERVED_APIS, $reported, 'the api table of the protocol document');
        self::assertCount(56, $reported, 'the zkBroker listener set of Kafka 2.8.2');
    }

    /**
     * The client sends version 2, whose answer is the version 1 answer: the throttle time closes it
     */
    public function testTheAnswerOfVersionTwoCarriesTheTrailingThrottleTime(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertSame(2, ApiVersionsRequest::VERSION, 'this line sends ApiVersions v2 (KIP-219)');
        self::assertSame(1, ApiVersionsRequestV1::VERSION);
        self::assertSame(0, ApiVersionsRequestV0::VERSION);
        self::assertSame(0, $response->throttleTimeMs, 'an ApiVersions request is never throttled without a quota');

        // The v1 frame is the v0 frame plus the four bytes of the throttle time - the one api of Kafka 0.11 that
        // appends the field instead of prepending it, so that the leading error code stays where a v0 client
        // expects it - and the v2 frame is the v1 frame, because KIP-219 changed the behaviour and not the schema
        $versionZero = $this->probe(ApiKeys::API_VERSIONS, 0, 3010)['body'];
        $versionOne  = $this->probe(ApiKeys::API_VERSIONS, 1, 3011)['body'];
        $versionTwo  = $this->probe(ApiKeys::API_VERSIONS, 2, 3012)['body'];

        self::assertSame(strlen($versionZero) + 4, strlen($versionOne));
        self::assertSame($versionZero, substr($versionOne, 0, -4));
        self::assertSame($versionOne, $versionTwo, 'the v2 answer is the v1 answer, byte for byte');
        self::assertSame(0, (int) unpack('N', substr($versionOne, -4))[1], 'the trailing throttle_time_ms');
    }

    public function testTheAdminClientReportsTheSameTable(): void
    {
        $apiVersions = $this->admin()->getApiVersions($this->anyNode());

        self::assertSame(array_keys(self::SERVED_APIS), array_keys($apiVersions));
        self::assertSame(ApiKeys::API_VERSIONS, $apiVersions[ApiKeys::API_VERSIONS]->apiKey);
        self::assertSame(0, $apiVersions[ApiKeys::CONTROLLED_SHUTDOWN]->minVersion, 'Kafka 1.0 serves v0 again');
    }

    public function testTheAnswerIsIndexedByApiKey(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertTrue($response->supports(ApiKeys::FETCH, 12), 'Fetch v12 arrived with Kafka 2.7 (KIP-482)');
        self::assertFalse($response->supports(ApiKeys::FETCH, 13), 'Fetch v13 is Kafka 3.1');
        self::assertTrue(
            $response->supports(ApiKeys::CONTROLLED_SHUTDOWN, 0),
            'the minimum version of key 7 is 0 again since Kafka 1.0'
        );
        self::assertSame(11, $response->maxVersionOf(ApiKeys::METADATA));
        self::assertSame(2, $response->maxVersionOf(ApiKeys::ELECT_LEADERS), 'ElectLeaders (43) is Kafka 2.2');
        self::assertSame(0, $response->maxVersionOf(ApiKeys::DESCRIBE_PRODUCERS), 'DescribeProducers (61) is 2.8');
        self::assertNull(
            $response->maxVersionOf(ApiKeys::VOTE),
            'the raft api 52 belongs to the controller listener of a KRaft cluster'
        );
        self::assertNull(
            $response->maxVersionOf(self::UNKNOWN_API_KEY),
            'the api key 65 (DescribeTransactions, Kafka 3.0) is above the table'
        );
    }

    /**
     * Every api key of the table, sent as a real frame at the **maximum version** the broker reports
     *
     * The maximum is the version that matters: it is the frame this line grows towards, it is the one that is
     * flexible wherever the api became flexible, and a wrong compact length or a forgotten tag buffer in it is not
     * an error the broker reports but a closed connection. The versions in between belong to the tickets that
     * implement them, each with its own vector.
     *
     * @return array<string, array{int, int}>
     */
    public static function servedApiProvider(): array
    {
        $cases = [];
        foreach (self::SERVED_APIS as $apiKey => [, $maxVersion]) {
            $flexible          = isset(self::FLEXIBLE_FROM[$apiKey]) && self::FLEXIBLE_FROM[$apiKey] <= $maxVersion;
            $suffix            = $flexible ? ' (flexible)' : '';
            $cases["key {$apiKey} v{$maxVersion}{$suffix}"] = [$apiKey, $maxVersion];
        }

        return $cases;
    }

    #[DataProvider('servedApiProvider')]
    public function testTheBrokerAnswersEveryApiAtItsMaximumVersion(int $apiKey, int $apiVersion): void
    {
        $correlationId = 1000 + $apiKey * 20 + $apiVersion;
        $result        = $this->probe($apiKey, $apiVersion, $correlationId);

        self::assertSame(
            RawApiProbe::ANSWERED,
            $result['status'],
            "The broker did not answer the api key {$apiKey} version {$apiVersion}"
        );
        self::assertSame($correlationId, $result['correlationId']);
    }

    /**
     * The first version above every api of the table
     *
     * ApiVersions (18) is the only api that is not here, because it is the only one whose unknown version is
     * answered instead of costing the connection; it has two tests of its own below.
     *
     * @return array<string, array{int, int}>
     */
    public static function unservedApiProvider(): array
    {
        $cases = [];
        foreach (self::SERVED_APIS as $apiKey => [, $maxVersion]) {
            if ($apiKey === ApiKeys::API_VERSIONS) {
                continue;
            }
            $cases["key {$apiKey} v" . ($maxVersion + 1)] = [$apiKey, $maxVersion + 1];
        }

        return $cases;
    }

    #[DataProvider('unservedApiProvider')]
    public function testTheBrokerClosesTheConnectionForAVersionAboveTheTable(int $apiKey, int $apiVersion): void
    {
        $result = $this->probe($apiKey, $apiVersion, 4000 + $apiKey * 20 + $apiVersion, withBody: false);

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "The broker did not close the connection for the api key {$apiKey} version {$apiVersion}, which Kafka "
            . '2.8.2 does not serve'
        );
        self::assertNull($result['correlationId'], 'a closed connection carries no response frame');
    }

    /**
     * The nine KRaft api keys of `ApiKeys.java` @ 2.8.2 are not enabled on a ZooKeeper-backed broker
     *
     * @return array<string, array{int}>
     */
    public static function unservedKeyProvider(): array
    {
        $cases = [];
        foreach (self::UNSERVED_KEYS as $apiKey) {
            $cases["key {$apiKey}"] = [$apiKey];
        }

        return $cases;
    }

    #[DataProvider('unservedKeyProvider')]
    public function testTheBrokerClosesTheConnectionForAnApiOfTheControllerListener(int $apiKey): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send($apiKey, 0, '', 5000 + $apiKey, RawApiProbe::HEADER_V2);
        $probe->close();

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "The broker did not close the connection for the api key {$apiKey}, which it does not serve"
        );
    }

    /**
     * A key above `ApiKeys.java` itself dies while the request *header* is parsed, not while the body is
     */
    public function testAnApiKeyAboveTheTableClosesTheConnection(): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(self::UNKNOWN_API_KEY, 0, '', 5100, RawApiProbe::HEADER_V2);
        $probe->close();

        self::assertSame(RawApiProbe::CLOSED, $result['status'], 'Unexpected api key: 65');
    }

    /**
     * ApiVersions is the one api whose unknown version is answered instead of costing the connection
     *
     * `RequestContext.parseRequest` @ 2.8.2 special-cases key 18 before it parses the body: it rebuilds the request
     * as an `ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0, header.apiVersion())`, so that
     * `KafkaApis.handleApiVersionsRequest` sees `hasUnsupportedRequestVersion` and answers `getErrorResponse()` -
     * the error code **35** written in the layout of **version 0**, i.e. without the throttle time that the version
     * 1 and 2 answers of the same broker carry.
     *
     * Since Kafka 2.4 (KIP-511) that answer is **not empty any more**: the array carries the single row of the
     * ApiVersions api itself, `18 0 3`, so that a client which guessed too high learns which version it should ask
     * for. A 1.1.1 broker answered `00 00 00 00` here.
     */
    public function testApiVersionsAnswersAnUnknownVersionWithTheErrorCode35(): void
    {
        $probe = new RawApiProbe(self::firstBootstrapServer());
        // The version the client asks for decides the header, so a v4 request carries the request header v2
        $result = $probe->send(ApiKeys::API_VERSIONS, 4, '', 3001, RawApiProbe::HEADER_V2);
        // The connection survives it, which is what makes the answer usable at all
        $served = $probe->send(ApiKeys::API_VERSIONS, 2, '', 3002);
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(3001, $result['correlationId']);
        self::assertSame(KafkaException::UNSUPPORTED_VERSION, self::errorCodeOf($result['body']));
        self::assertSame(
            12,
            strlen($result['body']),
            'an int16 error code and one api row, in the version 0 layout: no throttle time, no compact array'
        );
        self::assertSame(
            [ApiKeys::API_VERSIONS, 0, 3],
            array_values((array) unpack('n3', substr($result['body'], 6))),
            'KIP-511: the 35 names the version range of ApiVersions itself'
        );

        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(3002, $served['correlationId']);
    }

    /**
     * The header of an unknown ApiVersions version still has to be the one that version prescribes
     *
     * This is the trap KIP-482 set for a client that "just raises the version until the broker complains":
     * `RequestHeader.parse` @ 2.8.2 reads the api key and the api version first and asks
     * `ApiKeys.forId(apiKey).requestHeaderVersion(apiVersion)` how to read the rest, and for ApiVersions that
     * answer is **2** for every version above 2 - including the versions the broker does not serve. A v4 frame with
     * the plain header of the versions 0 to 2 therefore ends in `Error parsing request header. Our best guess of
     * the apiKey is: 18` and a closed connection, and only the same frame with an empty tag buffer behind the
     * client id is answered with the 35 above.
     */
    public function testAnUnknownApiVersionsVersionWithTheOlderHeaderCostsTheConnection(): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::API_VERSIONS, 4, '', 3003, RawApiProbe::HEADER_V1);
        $probe->close();

        self::assertSame(RawApiProbe::CLOSED, $result['status'], 'the header v2 is not optional above v2');
    }

    /**
     * ControlledShutdown answers all four of its versions, with three different request headers
     *
     * Key 7 is the api that carries the whole history of the request header: **v0** has no client id at all
     * ({@see RawApiProbe::HEADER_V0}, the `CONTROLLED_SHUTDOWN_V0_SCHEMA` of Kafka 1.0 and the
     * `"versions": "1+"` of the `ClientId` field @ 2.8.2), v1 and v2 use the common header, and **v3** - Kafka 2.4 -
     * is flexible and uses the header v2. Up to 0.11 the api was parsed by a Scala class that never validated the
     * version at all; since Kafka 1.0 it is an ordinary api that hangs up above its maximum.
     *
     * @return array<string, array{int, int}>
     */
    public static function controlledShutdownVersionProvider(): array
    {
        return [
            'v0, the header without a client id' => [0, RawApiProbe::HEADER_V0],
            'v1, the common header'              => [1, RawApiProbe::HEADER_V1],
            'v2, the broker epoch of KIP-380'    => [2, RawApiProbe::HEADER_V1],
            'v3, flexible (KIP-482)'             => [3, RawApiProbe::HEADER_V2],
        ];
    }

    #[DataProvider('controlledShutdownVersionProvider')]
    public function testControlledShutdownAnswersEveryOneOfItsVersions(int $apiVersion, int $headerVersion): void
    {
        $body = RawApiProbe::int32(self::UNKNOWN_BROKER_ID)
            . ($apiVersion >= 2 ? RawApiProbe::int64(-1) : '')
            . ($apiVersion >= 3 ? RawApiProbe::tagBuffer() : '');

        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::CONTROLLED_SHUTDOWN, $apiVersion, $body, 4200 + $apiVersion, $headerVersion);
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(
            KafkaException::BROKER_NOT_AVAILABLE,
            self::errorCodeOf($this->responseBody($result['body'], ApiKeys::CONTROLLED_SHUTDOWN, $apiVersion)),
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
     * A flexible frame is refused the same way when a compact length is off or a tag buffer is missing
     *
     * The two mistakes a client makes when it implements KIP-482: a compact array that announces `count + 1`
     * wrongly, and a structure that does not end in its tagged-field section. Both are read as a length that runs
     * past the frame, both end in `BufferUnderflowException` behind
     * `Error getting request for apiKey: DESCRIBE_GROUPS, apiVersion: 5` in the broker log, and both cost the
     * connection - there is no error code for a frame the broker cannot parse.
     *
     * @return array<string, array{string}>
     */
    public static function malformedFlexibleBodyProvider(): array
    {
        return [
            // A group array that claims three entries and carries none
            'a compact array count that is too large' => [
                RawApiProbe::compactArray(3) . RawApiProbe::boolean(false) . RawApiProbe::tagBuffer(),
            ],
            // The body of DescribeGroups v5 without its tagged-field section
            'a body without its tag buffer' => [
                RawApiProbe::compactArray(0) . RawApiProbe::boolean(false),
            ],
        ];
    }

    #[DataProvider('malformedFlexibleBodyProvider')]
    public function testAMalformedFlexibleBodyClosesTheConnection(string $body): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::DESCRIBE_GROUPS, 5, $body, 5002, RawApiProbe::HEADER_V2);
        $probe->close();

        self::assertSame(RawApiProbe::CLOSED, $result['status']);
    }

    /**
     * A closed connection costs nothing but that connection: the next one is answered normally
     */
    public function testAClosedConnectionDoesNotAffectTheNextOne(): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $closed = $probe->send(self::UNKNOWN_API_KEY, 0, '', 6001, RawApiProbe::HEADER_V2);
        $probe->close();

        $served = $this->probe(ApiKeys::METADATA, 11, 6002);

        self::assertSame(
            RawApiProbe::CLOSED,
            $closed['status'],
            'the api key 65 is above the table of Kafka 2.8.2'
        );
        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(6002, $served['correlationId']);
    }

    /**
     * The answer of a flexible version is compact from the response header on
     *
     * The two apis a producer and a consumer live on are the last ones KIP-482 reached, in Kafka 2.8 and 2.7, and
     * their empty answers are the shortest illustration of what a flexible frame looks like: the **response header
     * v1** is a correlation id and a tagged-field section, every array counts `count + 1` in an unsigned varint,
     * and every structure ends in a tag buffer of its own. An empty Produce v9 answer is therefore seven bytes
     * where the v5 answer of the 1.x line was eight, although it carries the same three fields.
     */
    public function testTheAnswerOfAFlexibleVersionIsCompactFromTheResponseHeaderOn(): void
    {
        $produce = $this->probe(ApiKeys::PRODUCE, 9, 7001)['body'];
        $fetch   = $this->probe(ApiKeys::FETCH, 12, 7002)['body'];

        //  00                the tag buffer of the response header v1
        //  01                [topic] = empty, as the compact count 0 + 1
        //  00 00 00 00       throttle_time_ms
        //  00                the tag buffer of the body
        self::assertSame('00' . '01' . '00000000' . '00', bin2hex($produce));

        //  00                the tag buffer of the response header v1
        //  00 00 00 00       throttle_time_ms
        //  00 00             error_code (KIP-227)
        //  00 00 00 00       session_id
        //  01                [topic] = empty
        //  00                the tag buffer of the body
        self::assertSame('00' . '00000000' . '0000' . '00000000' . '01' . '00', bin2hex($fetch));
    }

    /**
     * The ACL apis (29, 30, 31) are served, and answer 54 while the broker has no authorizer
     *
     * This is why the epic left them out of the line: `KafkaApis.handleDescribeAcls` @ 2.8.2 answers
     * `Errors.SECURITY_DISABLED` for every one of them unless `authorizer.class.name` is configured, so no wire
     * vector of a real answer can be captured on the shared container. The keys themselves are in `ApiKeys` and in
     * the api-key table, because the broker reports them.
     */
    public function testTheAclApisAreServedButDisabledWithoutAnAuthorizer(): void
    {
        $result = $this->probe(ApiKeys::DESCRIBE_ACLS, 2, 9129);
        $body   = $this->responseBody($result['body'], ApiKeys::DESCRIBE_ACLS, 2);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(0, self::throttleTimeOf($body));
        self::assertSame(
            KafkaException::SECURITY_DISABLED,
            self::errorCodeBehindTheThrottleTimeOf($body),
            'the container runs without an authorizer.class.name'
        );
    }

    /**
     * SaslHandshake, the other api Kafka 0.10.0 added, is answered on the plaintext listener with 34
     *
     * On a SASL listener the request never reaches the api layer: `SaslServerAuthenticator` intercepts it while the
     * connection is still unauthenticated and answers it there. Everything that does get through to
     * `KafkaApis.handleSaslHandshakeRequest` @ 2.8.2 is therefore a handshake that arrived at the wrong moment,
     * and that method answers a constant **34** (IllegalSaslState) with the mechanisms the broker has enabled - no
     * matter which mechanism was asked for. Key 17 is also one of the two apis of the table that KIP-482 never
     * reached, so its maximum version is still the plain v1 of Kafka 1.0.
     */
    public function testSaslHandshakeOnThePlaintextListenerIsAnsweredWithIllegalSaslState(): void
    {
        $versionOne = $this->probe(ApiKeys::SASL_HANDSHAKE, 1, 9002);

        self::assertSame(RawApiProbe::ANSWERED, $versionOne['status']);
        self::assertSame(KafkaException::ILLEGAL_SASL_STATE, self::errorCodeOf($versionOne['body']));
        self::assertArrayNotHasKey(
            ApiKeys::SASL_HANDSHAKE,
            self::FLEXIBLE_FROM,
            'SaslHandshake is one of the two apis that never became flexible'
        );
    }

    /**
     * SaslAuthenticate (36) on an already authenticated connection answers 34 with a message
     *
     * Like the handshake, the api never reaches `KafkaApis` on a SASL listener - `SaslServerAuthenticator` consumes
     * it while the connection is being authenticated. What arrives at `KafkaApis.handleSaslAuthenticateRequest`
     * @ 2.8.2 is therefore always a request that came too late, and the method answers **34** with the message
     * `SaslAuthenticate request received after successful authentication`. On the PLAINTEXT listener of the
     * container - where there is no authentication at all - that is the answer as well, which is what makes the key
     * safe to probe.
     */
    public function testSaslAuthenticateOnThePlaintextListenerIsAnsweredWithIllegalSaslState(): void
    {
        $result = $this->probe(ApiKeys::SASL_AUTHENTICATE, 2, 9003);
        $body   = $this->responseBody($result['body'], ApiKeys::SASL_AUTHENTICATE, 2);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(KafkaException::ILLEGAL_SASL_STATE, self::errorCodeOf($body));
        self::assertStringContainsString(
            'SaslAuthenticate request received after successful authentication',
            $body
        );
    }

    /**
     * The four delegation-token apis (38-41) are served and refuse a PLAINTEXT connection with 64
     *
     * KIP-48 lets a token be issued only over a channel that authenticated a real principal: `KafkaApis` @ 2.8.2
     * checks `isValidPrincipalType` and the security protocol of the listener first, and answers **64**
     * (`DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`) on PLAINTEXT and on a one-way SSL channel, before the token manager
     * is asked anything. The container does carry a `delegation.token.secret.key`, so the answer is not the 61 of a
     * broker without one.
     *
     * @return array<string, array{int}>
     */
    public static function delegationTokenApiProvider(): array
    {
        return [
            'CreateDelegationToken (38)'   => [ApiKeys::CREATE_DELEGATION_TOKEN],
            'RenewDelegationToken (39)'    => [ApiKeys::RENEW_DELEGATION_TOKEN],
            'ExpireDelegationToken (40)'   => [ApiKeys::EXPIRE_DELEGATION_TOKEN],
            'DescribeDelegationToken (41)' => [ApiKeys::DESCRIBE_DELEGATION_TOKEN],
        ];
    }

    #[DataProvider('delegationTokenApiProvider')]
    public function testTheDelegationTokenApisRefuseAPlaintextConnection(int $apiKey): void
    {
        $result = $this->probe($apiKey, 2, 9200 + $apiKey);
        $body   = $this->responseBody($result['body'], $apiKey, 2);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(
            KafkaException::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            self::errorCodeOf($body),
            'a delegation token is only issued over an authenticated channel'
        );
    }

    /**
     * Sends one probe request on a connection of its own and returns the result
     *
     * @param bool $withBody Whether to send the body of the version; a version above the table never gets that far
     *
     * @return array{status: string, correlationId: int|null, body: string}
     */
    private function probe(int $apiKey, int $apiVersion, int $correlationId, bool $withBody = true): array
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(
            $apiKey,
            $apiVersion,
            $withBody ? $this->body($apiKey, $apiVersion) : '',
            $correlationId,
            self::headerVersionOf($apiKey, $apiVersion)
        );
        $probe->close();

        return $result;
    }

    /**
     * Returns the version of the request header that a frame of this api version has to carry
     *
     * `ApiKeys.requestHeaderVersion()` @ 2.8.2: the header v0 for the one client-id-less frame of the protocol,
     * the header v2 from the first flexible version of the api on, and the common header v1 everywhere else. The
     * broker derives it from the version of the frame it is reading, served or not, so this holds for the versions
     * above the table as well.
     */
    private static function headerVersionOf(int $apiKey, int $apiVersion): int
    {
        if ($apiKey === ApiKeys::CONTROLLED_SHUTDOWN && $apiVersion === 0) {
            return RawApiProbe::HEADER_V0;
        }

        return ($apiVersion >= (self::FLEXIBLE_FROM[$apiKey] ?? PHP_INT_MAX))
            ? RawApiProbe::HEADER_V2
            : RawApiProbe::HEADER_V1;
    }

    /**
     * Strips the tagged-field section that the **response header v1** of a flexible answer carries
     *
     * `ApiKeys.responseHeaderVersion()` @ 2.8.2 answers 1 for every flexible version, so the body of such an answer
     * starts with the empty tag buffer of its header - one `00` byte in front of everything the api documents.
     * ApiVersions is the exception the generator writes out by hand: its answer always carries a response header
     * v0, so that a client can read the 35 of an older broker.
     */
    private function responseBody(string $body, int $apiKey, int $apiVersion): string
    {
        if ($apiKey === ApiKeys::API_VERSIONS || $apiVersion < (self::FLEXIBLE_FROM[$apiKey] ?? PHP_INT_MAX)) {
            return $body;
        }

        self::assertSame("\x00", substr($body, 0, 1), 'the response header v1 ends in an empty tag buffer');

        return substr($body, 1);
    }

    /**
     * Builds the smallest well-formed body of the **maximum** version of the given api that changes nothing
     *
     * Every api of the table is here, and the ones whose maximum version is flexible are built with the compact
     * types of KIP-482: `RawApiProbe::compactArray(0)` is the empty array (`01`), `compactString(null)` the null
     * string (`00`), and every structure - the body included - ends in an empty `tagBuffer()`.
     */
    private function body(int $apiKey, int $apiVersion): string
    {
        $emptyArray      = RawApiProbe::compactArray(0);
        $nullString      = RawApiProbe::compactString(null);
        $tag             = RawApiProbe::tagBuffer();
        $staleControl    = RawApiProbe::int32(-1) . RawApiProbe::int32(-1) . RawApiProbe::int64(-1);
        // No producer of the cluster owns this id, so every transaction api answers an error and changes nothing
        $unknownProducer = RawApiProbe::int64(-1) . RawApiProbe::int16(-1);

        return match ($apiKey) {
            // A null transactional id, acks 1, a timeout and no topic at all
            ApiKeys::PRODUCE => $nullString . RawApiProbe::int16(1) . RawApiProbe::int32(1000) . $emptyArray . $tag,
            // ReplicaId -1 (an ordinary consumer), the session of KIP-227 (id 0 and epoch -1 are the session-less
            // full fetch), no topic, no forgotten topic and the empty rack id of KIP-392
            ApiKeys::FETCH => RawApiProbe::int32(-1) . RawApiProbe::int32(100) . RawApiProbe::int32(0)
                . RawApiProbe::int32(1048576) . RawApiProbe::int8(0)
                . RawApiProbe::int32(0) . RawApiProbe::int32(-1)
                . $emptyArray . $emptyArray . RawApiProbe::compactString('') . $tag,
            ApiKeys::OFFSETS  => RawApiProbe::int32(-1) . RawApiProbe::int8(0) . $emptyArray . $tag,
            // An empty topic array is "no topic" since v1; `allow_auto_topic_creation` is false so that the probe
            // creates nothing, and the authorized operations of KIP-430 are not asked for
            ApiKeys::METADATA => $emptyArray . RawApiProbe::boolean(false) . RawApiProbe::boolean(false) . $tag,
            // The broker-to-broker apis: a controller epoch of -1 is stale, so the broker answers 11 and does
            // nothing; the broker epoch of KIP-380 is -1, which is "unknown" and never fences anything
            ApiKeys::LEADER_AND_ISR  => $staleControl . RawApiProbe::int8(0) . $emptyArray . $emptyArray . $tag,
            ApiKeys::STOP_REPLICA    => $staleControl . $emptyArray . $tag,
            ApiKeys::UPDATE_METADATA => $staleControl . $emptyArray . $emptyArray . $tag,
            ApiKeys::CONTROLLED_SHUTDOWN => RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . RawApiProbe::int64(-1)
                . $tag,
            ApiKeys::OFFSET_COMMIT => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('') . $nullString . $emptyArray . $tag,
            ApiKeys::OFFSET_FETCH => RawApiProbe::compactString($this->groupId) . $emptyArray
                . RawApiProbe::boolean(false) . $tag,
            // 0 is a group, 1 a transaction - the probe asks for a group, so that the `__transaction_state` topic
            // is not created behind its back
            ApiKeys::GROUP_COORDINATOR => RawApiProbe::compactString($this->groupId) . RawApiProbe::int8(0) . $tag,
            // A session timeout of 1 ms is below group.min.session.timeout.ms, so no group is created
            ApiKeys::JOIN_GROUP => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(1)
                . RawApiProbe::int32(1) . RawApiProbe::compactString('') . $nullString
                . RawApiProbe::compactString('consumer') . $emptyArray . $tag,
            ApiKeys::HEARTBEAT => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('probe-member') . $nullString . $tag,
            // KIP-345 turned the single member id of v0-v2 into a batch of member identities
            ApiKeys::LEAVE_GROUP => RawApiProbe::compactString($this->groupId) . $emptyArray . $tag,
            ApiKeys::SYNC_GROUP => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('probe-member') . $nullString . $nullString . $nullString
                . $emptyArray . $tag,
            ApiKeys::DESCRIBE_GROUPS => $emptyArray . RawApiProbe::boolean(false) . $tag,
            // KIP-518 added the states filter, which is empty here: every state
            ApiKeys::LIST_GROUPS => $emptyArray . $tag,
            // A mechanism the listener does not offer, so the broker answers 34 and creates nothing
            ApiKeys::SASL_HANDSHAKE => RawApiProbe::string('KAFKA-CLIENT-PROBE'),
            // The versions 0 to 2 have no body at all; v3 (KIP-511) carries the name and the version of the client
            // software as compact strings, and the broker matches both against `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`
            ApiKeys::API_VERSIONS   => $apiVersion >= 3
                ? RawApiProbe::compactString('kafka-client-php') . RawApiProbe::compactString('2.8') . $tag
                : '',
            // No topic to create and no topic to delete
            ApiKeys::CREATE_TOPICS => $emptyArray . RawApiProbe::int32(1000) . RawApiProbe::boolean(true) . $tag,
            ApiKeys::DELETE_TOPICS => $emptyArray . RawApiProbe::int32(1000) . $tag,
            // No log to truncate and no partition to look an epoch up in
            ApiKeys::DELETE_RECORDS => $emptyArray . RawApiProbe::int32(1000) . $tag,
            // -2 is the replica id of an ordinary consumer (KIP-320), the default of the field
            ApiKeys::OFFSET_FOR_LEADER_EPOCH => RawApiProbe::int32(-2) . $emptyArray . $tag,
            // A null transactional id asks for a bare producer id, which allocates one and creates no transaction
            ApiKeys::INIT_PRODUCER_ID => $nullString . RawApiProbe::int32(1000) . RawApiProbe::int64(-1)
                . RawApiProbe::int16(-1) . $tag,
            // Every transaction api names a transactional id the coordinator has never seen, with the producer id
            // -1: the answer is an error, and no transaction state is written
            ApiKeys::ADD_PARTITIONS_TO_TXN => RawApiProbe::compactString($this->transactionalId) . $unknownProducer
                . $emptyArray . $tag,
            ApiKeys::ADD_OFFSETS_TO_TXN => RawApiProbe::compactString($this->transactionalId) . $unknownProducer
                . RawApiProbe::compactString($this->groupId) . $tag,
            ApiKeys::END_TXN => RawApiProbe::compactString($this->transactionalId) . $unknownProducer
                . RawApiProbe::boolean(false) . $tag,
            ApiKeys::WRITE_TXN_MARKERS => $emptyArray . $tag,
            ApiKeys::TXN_OFFSET_COMMIT => RawApiProbe::compactString($this->transactionalId)
                . RawApiProbe::compactString($this->groupId) . $unknownProducer . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('') . $nullString . $emptyArray . $tag,
            // A filter that matches nothing, on a broker that has no authorizer at all; 1 is ANY for the resource
            // type, the pattern type of KIP-290, the operation and the permission type alike
            ApiKeys::DESCRIBE_ACLS => RawApiProbe::int8(1) . $nullString . RawApiProbe::int8(1) . $nullString
                . $nullString . RawApiProbe::int8(1) . RawApiProbe::int8(1) . $tag,
            ApiKeys::CREATE_ACLS => $emptyArray . $tag,
            ApiKeys::DELETE_ACLS => $emptyArray . $tag,
            // No resource to describe and none to change; v3 added the documentation of KIP-569
            ApiKeys::DESCRIBE_CONFIGS => $emptyArray . RawApiProbe::boolean(false) . RawApiProbe::boolean(false)
                . $tag,
            ApiKeys::ALTER_CONFIGS             => $emptyArray . RawApiProbe::boolean(true) . $tag,
            ApiKeys::INCREMENTAL_ALTER_CONFIGS => $emptyArray . RawApiProbe::boolean(true) . $tag,
            // No replica to move and no log directory to describe: an empty topic array is "nothing", where the
            // null array of DescribeLogDirs would ask for every partition of both log directories of the container
            ApiKeys::ALTER_REPLICA_LOG_DIRS => $emptyArray . $tag,
            ApiKeys::DESCRIBE_LOG_DIRS      => $emptyArray . $tag,
            // An empty token: the api is answered with 34 on every listener that did not authenticate with it
            ApiKeys::SASL_AUTHENTICATE => RawApiProbe::compactBytes('') . $tag,
            // No topic whose partition count should grow, and `validate_only` on top of that
            ApiKeys::CREATE_PARTITIONS => $emptyArray . RawApiProbe::int32(1000) . RawApiProbe::boolean(true) . $tag,
            // The token apis are refused on a PLAINTEXT connection before anything is read out of their body, so
            // the smallest well-formed frame of each is enough: no renewer, no hmac, no owner
            ApiKeys::CREATE_DELEGATION_TOKEN   => $emptyArray . RawApiProbe::int64(-1) . $tag,
            ApiKeys::RENEW_DELEGATION_TOKEN    => RawApiProbe::compactBytes('') . RawApiProbe::int64(-1) . $tag,
            ApiKeys::EXPIRE_DELEGATION_TOKEN   => RawApiProbe::compactBytes('') . RawApiProbe::int64(-1) . $tag,
            ApiKeys::DESCRIBE_DELEGATION_TOKEN => $emptyArray . $tag,
            // No group to delete and no group whose offsets should be deleted
            ApiKeys::DELETE_GROUPS => $emptyArray . $tag,
            ApiKeys::OFFSET_DELETE => RawApiProbe::string($this->groupId) . RawApiProbe::int32(0),
            // An empty partition list, never the null array that would elect a leader for every partition of the
            // cluster; 0 is the preferred-leader election of KIP-183
            ApiKeys::ELECT_LEADERS => RawApiProbe::int8(0) . $emptyArray . RawApiProbe::int32(1000) . $tag,
            // No partition to reassign and none to list - the null array would answer every reassignment there is
            ApiKeys::ALTER_PARTITION_REASSIGNMENTS => RawApiProbe::int32(1000) . $emptyArray . $tag,
            ApiKeys::LIST_PARTITION_REASSIGNMENTS  => RawApiProbe::int32(1000) . $emptyArray . $tag,
            // No quota component to match and no quota to alter (KIP-546)
            ApiKeys::DESCRIBE_CLIENT_QUOTAS => $emptyArray . RawApiProbe::boolean(false) . $tag,
            ApiKeys::ALTER_CLIENT_QUOTAS    => $emptyArray . RawApiProbe::boolean(true) . $tag,
            // No user to describe and no credential to change (KIP-554)
            ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS => $emptyArray . $tag,
            ApiKeys::ALTER_USER_SCRAM_CREDENTIALS    => $emptyArray . $emptyArray . $tag,
            // The broker-to-controller api of KIP-497: the broker epoch -1 is stale, so the answer is 77
            ApiKeys::ALTER_ISR => RawApiProbe::int32(-1) . RawApiProbe::int64(-1) . $emptyArray . $tag,
            // No feature to update (KIP-584), nothing to authorize (KIP-700) and no topic to describe (KIP-664)
            ApiKeys::UPDATE_FEATURES    => RawApiProbe::int32(1000) . $emptyArray . $tag,
            ApiKeys::DESCRIBE_CLUSTER   => RawApiProbe::boolean(false) . $tag,
            ApiKeys::DESCRIBE_PRODUCERS => $emptyArray . $tag,
            default                     => '',
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
     * Reads the leading `throttle_time_ms` int32 of an answer of Kafka 0.11 or later
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
    private function anyNode(): Node
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
