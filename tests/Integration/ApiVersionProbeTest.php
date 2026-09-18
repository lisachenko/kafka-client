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
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV2;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV3;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV1;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV2;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV3;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;

/**
 * Establishes which api keys and versions the client listener of a real Kafka 3.9.2 KRaft node serves.
 *
 * Kafka 0.10.0 added the api that answers that question - **ApiVersions**, key 18 - so this class no longer has to
 * guess it the way the `0.8.x` and `0.9.x` lines did. The first half of the suite asks the node with
 * {@see Client::apiVersions()} and pins its answer, which is the api-key table of `docs/protocol/3.9.md`: **61
 * keys**, the `broker` listener set of the JSON message specifications @ 3.9.2 minus the two telemetry apis and
 * the nine unstable ones ({@see self::SERVED_APIS}).
 *
 * The container is not the ZooKeeper-backed broker of the 2.x line any more but a **KRaft node in combined mode**
 * (`process.roles=broker,controller`): the same process answers on the client listeners 9092 to 9095 as a `broker`
 * and on the CONTROLLER listener 9096 as a `controller`, and Kafka 2.8 (KIP-500) made the ApiVersions answer a
 * property of the **listener** the request arrived on (`ApiVersionManager.apiVersionResponse` @ 3.9.2). Every
 * frame of this class is sent to 9092, so it sees the `broker` set and only that: the `zkBroker` apis of the
 * 2.x line - LeaderAndIsr, StopReplica, UpdateMetadata, ControlledShutdown, AlterPartition - are gone from the
 * answer together with everything the controller keeps for itself ({@see self::ZK_BROKER_LISTENER_KEYS},
 * {@see self::CONTROLLER_LISTENER_KEYS}).
 *
 * The second half is still a raw probe ({@see RawApiProbe}), because the *edges* of that table are not in it: what
 * the node does with a key or a version it does not serve is behaviour, not data. Kafka 0.10 changed that
 * behaviour and every release since keeps it, and this is where it is verified:
 *
 * * A 0.9.0.1 broker **dropped** a frame it could not parse and kept the connection open, so a client waited for its
 *   own timeout ({@see RawApiProbe::SILENT}).
 * * A 3.9.2 node **closes the connection** ({@see RawApiProbe::CLOSED}). `SocketServer.parseRequestHeader`
 *   @ 3.9.2 asks `ApiVersionManager.isApiEnabled(apiKey, apiVersion)` right behind `RequestHeader.parse`, and
 *   that one check now covers what two exceptions covered on 2.8.2: a key of another listener, an unstable key
 *   and a version above the table all end in `Received request api key VOTE with version 0 which is not enabled`,
 *   a key no `ApiKeys` entry knows ends in `Error parsing request header. Our best guess of the apiKey is: 88`,
 *   and a body the generated message class cannot read still ends in `Error getting request for apiKey:
 *   GET_TELEMETRY_SUBSCRIPTIONS, apiVersion: 0` with a `BufferUnderflowException` behind it.
 *   `processCompletedReceives` catches the `InvalidRequestException` of all three and calls `close()` on the
 *   channel; `docker logs kafka-3-9-2` shows `ERROR Closing socket for ... because of error` with the reason.
 *
 * **Exactly one api is an exception to that rule**: ApiVersions itself, which answers an unknown version with the
 * error code 35 on a connection that stays open - `ApiKeys.isVersionEnabled` @ 3.9.2 returns `true` for key 18
 * before it looks at the version - as long as the frame carries the request header that the *requested* version
 * prescribes, which since KIP-482 is the header v2 for every version above 2.
 *
 * The frame of every key is sent at the **maximum version the node reports**, which for 59 of the 61 keys is a
 * **flexible** version (KIP-482): a compact body, the request header v2 and a tagged-field section at the end of
 * every structure. The bytes are built by the fixture, not by the schema engine, exactly as the frames above the
 * table are: a probe that used the engine could only send what the engine believes, and half of what this class
 * checks is what happens to a frame no class of this package can build.
 *
 * The probe never changes the state of the cluster: the group and transaction apis use a group id and a
 * transactional id that no other test uses, UnregisterBroker and the raft-voter apis name a broker id that does not
 * exist and a cluster id that is not this cluster's, and every api that takes a list of things to change -
 * CreateTopics, DeleteTopics, DeleteRecords, WriteTxnMarkers, AlterConfigs, ElectLeaders,
 * AlterPartitionReassignments, the quota, SCRAM and ACL apis, UpdateFeatures - is sent with an empty array or
 * `validate_only`. Two apis of this node **do not answer an empty array at all** and are sent with one harmless
 * element instead ({@see self::body()}).
 *
 * @see docs/protocol/3.9.md, section "API keys"
 */
#[CoversClass(ApiKeys::class)]
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsRequestV0::class)]
#[CoversClass(ApiVersionsRequestV1::class)]
#[CoversClass(ApiVersionsRequestV2::class)]
#[CoversClass(ApiVersionsRequestV3::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseV0::class)]
#[CoversClass(ApiVersionsResponseV1::class)]
#[CoversClass(ApiVersionsResponseV2::class)]
#[CoversClass(ApiVersionsResponseV3::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
final class ApiVersionProbeTest extends IntegrationTestCase
{
    /**
     * An unknown broker id, so that UnregisterBroker reports an error instead of unregistering the node
     */
    private const int UNKNOWN_BROKER_ID = 4242;

    /**
     * A cluster id that is not the one of the container, so that the raft-voter apis refuse the frame outright
     */
    private const string FOREIGN_CLUSTER_ID = 'not-this-cluster';

    /**
     * The name of the one feature a 3.9.2 node finalizes (KIP-584); its level 21 is `3.9-IV0` (`MetadataVersion.java`)
     */
    private const string METADATA_VERSION_FEATURE = 'metadata.version';

    /**
     * The feature of KIP-853 that a node only reports to an **ApiVersions v4**, because its minimum is 0
     *
     * `kraft.version` 0 is the static voter set of KIP-595 (`controller.quorum.voters`) and 1 the reconfigurable
     * one the raft-voter apis 80 and 81 change. The node supports 0 to 1 and has finalized 0 (KAFKA-17011).
     */
    private const string KRAFT_VERSION_FEATURE = 'kraft.version';

    /**
     * The api table of the client listener of a Kafka 3.9.2 KRaft node, as `api key => [minimum, maximum]`
     *
     * This is what the `validVersions` of the JSON message specifications @ 3.9.2 declare for every api whose
     * `listeners` include `broker`, and what the container really answers. **Every minimum is 0** up to and
     * including 3.9: the tag `4.0.0` is the first whose specifications start above it (Produce at 3, Fetch at 4,
     * KIP-896).
     *
     * The set is the one of the **listener** the request arrived on (`ApiVersionManager.apiVersionResponse`
     * @ 3.9.2): `DefaultApiVersionManager` starts from `ApiKeys.apisForListener(BROKER)`, intersects every
     * forwardable api with what the active controller serves (`ApiVersionsResponse.intersectForwardableApis`)
     * and drops two more kinds of key from the answer before it is sent - the telemetry apis 71 and 72 while no
     * client-metrics receiver plugin is configured, and every api whose only version is `latestVersionUnstable`
     * while `unstable.api.versions.enable` is off ({@see self::HIDDEN_TELEMETRY_KEYS}, {@see self::UNSTABLE_KEYS}).
     * That leaves these 61 keys.
     */
    private const array SERVED_APIS = [
        ApiKeys::PRODUCE                        => [0, 11],
        ApiKeys::FETCH                          => [0, 17],
        ApiKeys::OFFSETS                        => [0, 9],
        ApiKeys::METADATA                       => [0, 12],
        ApiKeys::OFFSET_COMMIT                  => [0, 9],
        ApiKeys::OFFSET_FETCH                   => [0, 9],
        ApiKeys::GROUP_COORDINATOR              => [0, 6],
        ApiKeys::JOIN_GROUP                     => [0, 9],
        ApiKeys::HEARTBEAT                      => [0, 4],
        ApiKeys::LEAVE_GROUP                    => [0, 5],
        ApiKeys::SYNC_GROUP                     => [0, 5],
        ApiKeys::DESCRIBE_GROUPS                => [0, 5],
        ApiKeys::LIST_GROUPS                    => [0, 5],
        ApiKeys::SASL_HANDSHAKE                 => [0, 1],
        ApiKeys::API_VERSIONS                   => [0, 4],
        ApiKeys::CREATE_TOPICS                  => [0, 7],
        ApiKeys::DELETE_TOPICS                  => [0, 6],
        ApiKeys::DELETE_RECORDS                 => [0, 2],
        ApiKeys::INIT_PRODUCER_ID               => [0, 5],
        ApiKeys::OFFSET_FOR_LEADER_EPOCH        => [0, 4],
        ApiKeys::ADD_PARTITIONS_TO_TXN          => [0, 5],
        ApiKeys::ADD_OFFSETS_TO_TXN             => [0, 4],
        ApiKeys::END_TXN                        => [0, 4],
        ApiKeys::WRITE_TXN_MARKERS              => [0, 1],
        ApiKeys::TXN_OFFSET_COMMIT              => [0, 4],
        ApiKeys::DESCRIBE_ACLS                  => [0, 3],
        ApiKeys::CREATE_ACLS                    => [0, 3],
        ApiKeys::DELETE_ACLS                    => [0, 3],
        ApiKeys::DESCRIBE_CONFIGS               => [0, 4],
        ApiKeys::ALTER_CONFIGS                  => [0, 2],
        ApiKeys::ALTER_REPLICA_LOG_DIRS         => [0, 2],
        ApiKeys::DESCRIBE_LOG_DIRS              => [0, 4],
        ApiKeys::SASL_AUTHENTICATE              => [0, 2],
        ApiKeys::CREATE_PARTITIONS              => [0, 3],
        ApiKeys::CREATE_DELEGATION_TOKEN        => [0, 3],
        ApiKeys::RENEW_DELEGATION_TOKEN         => [0, 2],
        ApiKeys::EXPIRE_DELEGATION_TOKEN        => [0, 2],
        ApiKeys::DESCRIBE_DELEGATION_TOKEN      => [0, 3],
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
        ApiKeys::DESCRIBE_QUORUM                => [0, 2],
        ApiKeys::UPDATE_FEATURES                => [0, 1],
        ApiKeys::DESCRIBE_CLUSTER               => [0, 1],
        ApiKeys::DESCRIBE_PRODUCERS             => [0, 0],
        ApiKeys::UNREGISTER_BROKER              => [0, 0],
        ApiKeys::DESCRIBE_TRANSACTIONS          => [0, 0],
        ApiKeys::LIST_TRANSACTIONS              => [0, 1],
        ApiKeys::CONSUMER_GROUP_HEARTBEAT       => [0, 0],
        ApiKeys::CONSUMER_GROUP_DESCRIBE        => [0, 0],
        ApiKeys::LIST_CLIENT_METRICS_RESOURCES  => [0, 0],
        ApiKeys::DESCRIBE_TOPIC_PARTITIONS      => [0, 0],
        ApiKeys::ADD_RAFT_VOTER                 => [0, 0],
        ApiKeys::REMOVE_RAFT_VOTER              => [0, 0],
    ];

    /**
     * The first **flexible** version of every api of the table that has one, as `api key => version` (KIP-482)
     *
     * The `flexibleVersions` of the JSON message specification of the request @ 3.9.2, verified frame by frame
     * against the container: a version below this one is refused when it is sent with a compact body, and a version
     * from it on is refused when it is not. Only two apis of the table never became flexible - SaslHandshake (17),
     * which was frozen when SaslAuthenticate took over the token exchange, and OffsetDelete (47), the one api of
     * Kafka 2.4 that was written without it - and they are simply absent here. Every api that Kafka 2.8 or a 3.x
     * release added is flexible from its version 0.
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
        ApiKeys::DESCRIBE_QUORUM                => 0,
        ApiKeys::UPDATE_FEATURES                => 0,
        ApiKeys::DESCRIBE_CLUSTER               => 0,
        ApiKeys::DESCRIBE_PRODUCERS             => 0,
        ApiKeys::UNREGISTER_BROKER              => 0,
        ApiKeys::DESCRIBE_TRANSACTIONS          => 0,
        ApiKeys::LIST_TRANSACTIONS              => 0,
        ApiKeys::CONSUMER_GROUP_HEARTBEAT       => 0,
        ApiKeys::CONSUMER_GROUP_DESCRIBE        => 0,
        ApiKeys::LIST_CLIENT_METRICS_RESOURCES  => 0,
        ApiKeys::DESCRIBE_TOPIC_PARTITIONS      => 0,
        ApiKeys::ADD_RAFT_VOTER                 => 0,
        ApiKeys::REMOVE_RAFT_VOTER              => 0,
    ];

    /**
     * The api keys of `ApiKeys.java` @ 3.9.2 whose `listeners` name `zkBroker` but not `broker`
     *
     * LeaderAndIsr (4), StopReplica (5) and UpdateMetadata (6) are the controller-to-broker apis of a ZooKeeper
     * cluster and list `["zkBroker"]` alone; ControlledShutdown (7) and AlterPartition (56, the `AlterIsr` of 2.8,
     * KIP-704) list `["zkBroker", "controller"]`, so a KRaft broker sends them to its controller listener and never
     * receives them itself. All five were in the table of the 2.x line, because that container was a ZooKeeper
     * broker; on the client listener of a KRaft node a frame of any of them costs the connection: `Received request
     * api key CONTROLLED_SHUTDOWN with version 0 which is not enabled`.
     */
    private const array ZK_BROKER_LISTENER_KEYS = [
        ApiKeys::LEADER_AND_ISR,
        ApiKeys::STOP_REPLICA,
        ApiKeys::UPDATE_METADATA,
        ApiKeys::CONTROLLED_SHUTDOWN,
        ApiKeys::ALTER_ISR,
    ];

    /**
     * The api keys of `ApiKeys.java` @ 3.9.2 that only the CONTROLLER listener serves
     *
     * The raft apis of KIP-595 (52 to 54), `FetchSnapshot` (59), the broker registration and heartbeat of KIP-631
     * (62, 63), `ControllerRegistration` (70, KIP-919), `AssignReplicasToDirs` (73, KIP-858) and `UpdateRaftVoter`
     * (82, KIP-853) list `["controller"]` alone; the `Envelope` of KIP-590 (58) and `AllocateProducerIds` (67,
     * KIP-730) list `["controller", "zkBroker"]` - a ZooKeeper broker forwards through the one and asks for
     * producer ids with the other, a KRaft broker does both over its own controller channel. The container serves
     * all of them on 9096 and none on 9092: `Received request api key VOTE with version 0 which is not enabled`.
     * DescribeQuorum (55), UnregisterBroker (64), AddRaftVoter (80) and RemoveRaftVoter (81) are the four
     * controller apis whose `listeners` include `broker` as well, and they are in the table, forwarded by the
     * broker to the controller (`forwardToControllerOrFail` in `KafkaApis` @ 3.9.2).
     */
    private const array CONTROLLER_LISTENER_KEYS = [
        ApiKeys::VOTE,
        ApiKeys::BEGIN_QUORUM_EPOCH,
        ApiKeys::END_QUORUM_EPOCH,
        ApiKeys::ENVELOPE,
        ApiKeys::FETCH_SNAPSHOT,
        ApiKeys::BROKER_REGISTRATION,
        ApiKeys::BROKER_HEARTBEAT,
        ApiKeys::ALLOCATE_PRODUCER_IDS,
        ApiKeys::CONTROLLER_REGISTRATION,
        ApiKeys::ASSIGN_REPLICAS_TO_DIRS,
        ApiKeys::UPDATE_RAFT_VOTER,
    ];

    /**
     * The two client-telemetry apis of KIP-714 (Kafka 3.7), served but hidden from the table
     *
     * GetTelemetrySubscriptions (71) and PushTelemetry (72) list `["broker"]` and are enabled on the listener, so a
     * frame of either passes `isApiEnabled` and reaches `KafkaApis`; but `ApiVersionsResponse.filterApis` @ 3.9.2
     * skips both rows of the answer while `ClientMetricsManager.isTelemetryReceiverConfigured` is false, i.e. while
     * no `MetricsReporter` that implements `ClientTelemetry` is configured - which is the case of the container.
     * The rest of the table is what KIP-714 promises a client that finds no row 71: "do not send telemetry".
     */
    private const array HIDDEN_TELEMETRY_KEYS = [
        ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS,
        ApiKeys::PUSH_TELEMETRY,
    ];

    /**
     * The api keys of `ApiKeys.java` @ 3.9.2 whose only version is `"latestVersionUnstable": true`
     *
     * The share-group apis of KIP-932 (76 to 79) and the share-group state apis of KIP-932 as well (83 to 87) are
     * early-access in 3.9: their specifications carry the flag, `ApiKeys.latestVersion(false)` answers -1 for
     * them, `toApiVersion` drops them from the table and `isVersionEnabled(0, false)` refuses every frame while
     * `unstable.api.versions.enable` is off - which it is in the container - with the same closed connection as a
     * key of another listener: `Received request api key SHARE_FETCH with version 0 which is not enabled`.
     *
     * @see docs/protocol/3.9.md, section "The raft-voter apis (keys 80 and 81) and the share groups — probe only"
     */
    private const array UNSTABLE_KEYS = [
        ApiKeys::SHARE_GROUP_HEARTBEAT,
        ApiKeys::SHARE_GROUP_DESCRIBE,
        ApiKeys::SHARE_FETCH,
        ApiKeys::SHARE_ACKNOWLEDGE,
        ApiKeys::INITIALIZE_SHARE_GROUP_STATE,
        ApiKeys::READ_SHARE_GROUP_STATE,
        ApiKeys::WRITE_SHARE_GROUP_STATE,
        ApiKeys::DELETE_SHARE_GROUP_STATE,
        ApiKeys::READ_SHARE_GROUP_STATE_SUMMARY,
    ];

    /**
     * The first api key above the table of `ApiKeys.java` @ 3.9.2, which ends at `ReadShareGroupStateSummary` (87)
     *
     * No 3.9.2 node can name it: `ApiKeys.forId(88)` throws, and the node closes the connection while it parses
     * the request header (`Error parsing request header. Our best guess of the apiKey is: 88`).
     */
    private const int UNKNOWN_API_KEY = 88;

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

    /**
     * Topic name of this test class; it never exists, DescribeTopicPartitions asks for it and gets the error 3
     */
    private string $missingTopic;

    protected function setUp(): void
    {
        parent::setUp();

        $suffix                = bin2hex(random_bytes(6));
        $this->groupId         = 't1-probe-' . $suffix;
        $this->transactionalId = 't1-probe-txn-' . $suffix;
        $this->missingTopic    = 't1-probe-no-such-topic-' . $suffix;
    }

    public function testTheBrokerReportsEveryApiOfKafka392(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);

        $reported = [];
        foreach ($response->apiVersions as $apiKey => $metadata) {
            $reported[$apiKey] = [$metadata->minVersion, $metadata->maxVersion];
        }

        self::assertSame(self::SERVED_APIS, $reported, 'the api table of the protocol document');
        self::assertCount(61, $reported, 'the broker listener set of Kafka 3.9.2 without telemetry and unstable apis');
    }

    /**
     * The client sends version 4, and every version of the answer closes with the throttle time
     */
    public function testTheAnswerCarriesTheTrailingThrottleTimeInEveryVersion(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertSame(4, ApiVersionsRequest::VERSION, 'this line sends the ApiVersions v4 of KAFKA-17011');
        self::assertSame(3, ApiVersionsRequestV3::VERSION, 'the flexible frame of KIP-511 it is built on');
        self::assertSame(2, ApiVersionsRequestV2::VERSION);
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

    /**
     * A KRaft node finalizes features, so the tagged fields of KIP-584 are no longer empty
     *
     * The ZooKeeper broker of the 2.x line answered one tagged field, the finalized-features epoch 0. A KRaft node
     * keeps its features in the metadata log and answers all three: the features it **supports** (tag 0), the
     * epoch of the finalized ones (tag 1, the offset of the metadata log and therefore never the same twice) and
     * the features the cluster has **finalized** (tag 2). The **version 4** of Kafka 3.9 is what this client sends,
     * and it sees **two** supported features - `kraft.version` 0 to 1 and `metadata.version` 1 to 21 - where a v3
     * request is shown one: a feature whose minimum is 0 is filtered out of every answer below the version 4
     * (KAFKA-17011, {@see self::testApiVersionsVersionFourUnhidesTheSupportedFeaturesWithAMinimumOfZero()}).
     * `metadata.version` is finalized at the level **21**, which is `3.9-IV0` in `MetadataVersion.java` @ 3.9.2,
     * the `LATEST_PRODUCTION` of the release; the minimum it supports is level 1, `3.0-IV1`. `kraft.version` is
     * finalized at **0**, and a feature at the level 0 is not in the finalized map at all - which is why the static
     * `controller.quorum.voters` of KIP-595 still govern this quorum. The fourth tag of the specification,
     * `zk_migration_ready` (3), stays at its default `false` and is therefore not written at all.
     */
    public function testTheTaggedFieldsOfTheAnswerCarryTheFinalizedFeaturesOfTheKRaftNode(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertGreaterThanOrEqual(0, $response->finalizedFeaturesEpoch, 'a KRaft node knows its epoch');
        self::assertSame(
            [self::KRAFT_VERSION_FEATURE, self::METADATA_VERSION_FEATURE],
            array_keys($response->supportedFeatures),
            'the version 4 sees the feature whose minimum is 0 as well'
        );
        self::assertSame(0, $response->supportedFeatures[self::KRAFT_VERSION_FEATURE]->minVersion, 'KAFKA-17011');
        self::assertSame(1, $response->supportedFeatures[self::KRAFT_VERSION_FEATURE]->maxVersion, 'KIP-853');
        self::assertSame(1, $response->supportedFeatures[self::METADATA_VERSION_FEATURE]->minVersion, '3.0-IV1');
        self::assertSame(21, $response->supportedFeatures[self::METADATA_VERSION_FEATURE]->maxVersion, '3.9-IV0');
        self::assertSame(
            [self::METADATA_VERSION_FEATURE],
            array_keys($response->finalizedFeatures),
            'kraft.version is finalized at the level 0, and a feature at the level 0 is not finalized at all'
        );
        self::assertSame(21, $response->finalizedFeatures[self::METADATA_VERSION_FEATURE]->maxVersionLevel);
        self::assertSame(21, $response->finalizedFeatures[self::METADATA_VERSION_FEATURE]->minVersionLevel);
    }

    public function testTheAdminClientReportsTheSameTable(): void
    {
        $apiVersions = $this->admin()->getApiVersions($this->anyNode());

        self::assertSame(array_keys(self::SERVED_APIS), array_keys($apiVersions));
        self::assertSame(ApiKeys::API_VERSIONS, $apiVersions[ApiKeys::API_VERSIONS]->apiKey);
        self::assertSame(
            0,
            $apiVersions[ApiKeys::PRODUCE]->minVersion,
            'Kafka 3.9 still serves Produce v0; the specification @ 4.0.0 is the first that starts at v3 (KIP-896)'
        );
    }

    public function testTheAnswerIsIndexedByApiKey(): void
    {
        $response = $this->client()->apiVersions($this->anyNode());

        self::assertTrue($response->supports(ApiKeys::FETCH, 17), 'Fetch v17 arrived with Kafka 3.9 (KIP-853)');
        self::assertFalse($response->supports(ApiKeys::FETCH, 18), 'no specification up to 4.0.0 has a Fetch v18');
        self::assertSame(12, $response->maxVersionOf(ApiKeys::METADATA), 'Metadata v12 is Kafka 3.1 (topic ids)');
        self::assertSame(2, $response->maxVersionOf(ApiKeys::ELECT_LEADERS), 'ElectLeaders (43) is Kafka 2.2');
        self::assertSame(
            0,
            $response->maxVersionOf(ApiKeys::DESCRIBE_TOPIC_PARTITIONS),
            'DescribeTopicPartitions (75) is Kafka 3.8'
        );
        self::assertNull(
            $response->maxVersionOf(ApiKeys::CONTROLLED_SHUTDOWN),
            'key 7 belongs to the zkBroker and controller listeners, not to the client listener of a KRaft node'
        );
        self::assertNull(
            $response->maxVersionOf(ApiKeys::VOTE),
            'the raft api 52 belongs to the controller listener'
        );
        self::assertNull(
            $response->maxVersionOf(ApiKeys::SHARE_FETCH),
            'the unstable api 78 is hidden without unstable.api.versions.enable'
        );
        self::assertNull(
            $response->maxVersionOf(self::UNKNOWN_API_KEY),
            'the api key 88 is above the table of ApiKeys.java @ 3.9.2'
        );
    }

    /**
     * Every api key of the table, sent as a real frame at the **maximum version** the node reports
     *
     * The maximum is the version that matters: it is the frame this line grows towards, it is the one that is
     * flexible wherever the api became flexible, and a wrong compact length or a forgotten tag buffer in it is not
     * an error the node reports but a closed connection. The versions in between belong to the tickets that
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

    /**
     * On 2.8.2 the version check was the `UnsupportedVersionException` of the generated message class; on 3.9.2 it
     * is `ApiVersionManager.isApiEnabled` in the network thread, and the log says `Received request api key
     * OFFSET_COMMIT with version 10 which is not enabled` before any body is looked at
     */
    #[DataProvider('unservedApiProvider')]
    public function testTheBrokerClosesTheConnectionForAVersionAboveTheTable(int $apiKey, int $apiVersion): void
    {
        $result = $this->probe($apiKey, $apiVersion, 4000 + $apiKey * 20 + $apiVersion, withBody: false);

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "The broker did not close the connection for the api key {$apiKey} version {$apiVersion}, which Kafka "
            . '3.9.2 does not serve'
        );
        self::assertNull($result['correlationId'], 'a closed connection carries no response frame');
    }

    /**
     * The sixteen api keys of `ApiKeys.java` @ 3.9.2 that another listener of the node serves and 9092 does not
     *
     * @return array<string, array{int}>
     */
    public static function otherListenerKeyProvider(): array
    {
        $cases = [];
        foreach (self::ZK_BROKER_LISTENER_KEYS as $apiKey) {
            $cases["key {$apiKey} (zkBroker)"] = [$apiKey];
        }
        foreach (self::CONTROLLER_LISTENER_KEYS as $apiKey) {
            $cases["key {$apiKey} (controller)"] = [$apiKey];
        }

        return $cases;
    }

    #[DataProvider('otherListenerKeyProvider')]
    public function testTheBrokerClosesTheConnectionForAnApiOfAnotherListener(int $apiKey): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send($apiKey, 0, '', 5000 + $apiKey, RawApiProbe::HEADER_V2);
        $probe->close();

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "The broker did not close the connection for the api key {$apiKey}, which the client listener does "
            . 'not serve'
        );
    }

    /**
     * The nine unstable api keys of `ApiKeys.java` @ 3.9.2: the share groups of KIP-932 (76 to 79, 83 to 87)
     *
     * Share groups are early access in Kafka 3.9 and **out of this line by decision of the owner**, and this is
     * the measurement that decision rests on: a ShareGroupHeartbeat v0 - or any of the other eight - does not get
     * an error code, it gets the connection closed, exactly like a key of another listener, because
     * `ApiKeys.toApiVersion(false)` leaves an api whose only version is `latestVersionUnstable` out of the table
     * and `isApiEnabled` then refuses every version of it. The node logs `Received request api key
     * SHARE_GROUP_HEARTBEAT with version 0 which is not enabled`. The four share-group error codes 121 to 124 are
     * therefore declared on this line and unreachable on this node.
     *
     * @return array<string, array{int}>
     */
    public static function unstableKeyProvider(): array
    {
        $cases = [];
        foreach (self::UNSTABLE_KEYS as $apiKey) {
            $cases["key {$apiKey}"] = [$apiKey];
        }

        return $cases;
    }

    #[DataProvider('unstableKeyProvider')]
    public function testTheBrokerClosesTheConnectionForAnUnstableApi(int $apiKey): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send($apiKey, 0, '', 5200 + $apiKey, RawApiProbe::HEADER_V2);
        $probe->close();

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "The broker did not close the connection for the unstable api key {$apiKey}"
        );
    }

    /**
     * The telemetry apis are answered although the table does not list them
     *
     * A frame of GetTelemetrySubscriptions v0 with the all-zero client instance id - "I have no id yet", the first
     * contact of KIP-714 - passes `isApiEnabled`, reaches `KafkaApis.handleGetTelemetrySubscriptionsRequest`
     * @ 3.9.2 and is answered by the `ClientMetricsManager` every KRaft broker has, receiver plugin or not: error
     * **0**, a freshly generated client instance id, a subscription id, the four codecs the node accepts as
     * `[zstd, lz4, gzip, snappy]` (4, 3, 1, 2), a push interval of 300000 ms, 1048576 bytes at most, delta
     * temporality and no metric at all - there is no subscription to match. The instance lives in an in-memory
     * cache of the manager, expires on its own and touches nothing on disk; a second frame with the same id inside
     * the push interval is answered **89** (ThrottlingQuotaExceeded), which is why the probe never reuses one.
     *
     * PushTelemetry v0 with the all-zero id is refused with **42** (InvalidRequest) before any instance is looked
     * up, and ListClientMetricsResources (74, in the table) answers 0 with no resource, because
     * `kafka-client-metrics.sh` never configured one. An empty body of 71 or 72 is not "the api is disabled" but a
     * `BufferUnderflowException` - the uuid is 16 bytes the parser insists on - and costs the connection like
     * every malformed body.
     */
    public function testTheTelemetryApisAreServedAlthoughTheTableHidesThem(): void
    {
        $zeroUuid = str_repeat("\x00", 16);
        $probe    = new RawApiProbe(self::firstBootstrapServer());

        $subscriptions = $probe->send(
            ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS,
            0,
            $zeroUuid . RawApiProbe::tagBuffer(),
            5300,
            RawApiProbe::HEADER_V2
        );
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $subscriptions['status']);
        self::assertSame(5300, $subscriptions['correlationId']);
        $body = $this->responseBody($subscriptions['body'], ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS, 0);
        self::assertSame(0, self::throttleTimeOf($body));
        self::assertSame(KafkaException::NO_ERROR, self::errorCodeBehindTheThrottleTimeOf($body));
        self::assertNotSame($zeroUuid, substr($body, 6, 16), 'the node generated a client instance id');
        //  05 04 03 01 02    accepted_compression_types = [zstd, lz4, gzip, snappy], as the compact count 4 + 1
        //  00 04 93 e0       push_interval_ms = 300000
        //  00 10 00 00       telemetry_max_bytes = 1048576
        //  01                delta_temporality = true
        //  01                requested_metrics = empty
        //  00                the tag buffer of the body
        self::assertSame(
            '0504030102' . '000493e0' . '00100000' . '01' . '01' . '00',
            bin2hex(substr($body, 26)),
            'the subscription of a node without a telemetry receiver'
        );

        $probe = new RawApiProbe(self::firstBootstrapServer());
        $push  = $probe->send(
            ApiKeys::PUSH_TELEMETRY,
            0,
            $zeroUuid . RawApiProbe::int32(0) . RawApiProbe::boolean(false) . RawApiProbe::int8(0)
                . RawApiProbe::compactBytes('') . RawApiProbe::tagBuffer(),
            5301,
            RawApiProbe::HEADER_V2
        );
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $push['status']);
        self::assertSame(5301, $push['correlationId']);
        self::assertSame(
            KafkaException::INVALID_REQUEST,
            self::errorCodeBehindTheThrottleTimeOf($this->responseBody($push['body'], ApiKeys::PUSH_TELEMETRY, 0)),
            'the all-zero client instance id is reserved'
        );

        $resources = $this->probe(ApiKeys::LIST_CLIENT_METRICS_RESOURCES, 0, 5302);
        self::assertSame(RawApiProbe::ANSWERED, $resources['status']);
        self::assertSame(
            '00000000' . '0000' . '01' . '00',
            bin2hex($this->responseBody($resources['body'], ApiKeys::LIST_CLIENT_METRICS_RESOURCES, 0)),
            'no throttle, no error, no client metrics resource'
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

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            'Error parsing request header. Our best guess of the apiKey is: 88'
        );
    }

    /**
     * ApiVersions is the one api whose unknown version is answered instead of costing the connection
     *
     * `ApiKeys.isVersionEnabled` @ 3.9.2 lets every version of key 18 through the network thread, and
     * `RequestContext.parseRequest` special-cases it before it parses the body: it rebuilds the request as an
     * `ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0, header.apiVersion())`, so that
     * `KafkaApis.handleApiVersionsRequest` sees `hasUnsupportedRequestVersion` and answers `getErrorResponse()` -
     * the error code **35** written in the layout of **version 0**, i.e. without the throttle time that the version
     * 1 to 4 answers of the same node carry.
     *
     * Since Kafka 2.4 (KIP-511) that answer is **not empty any more**: the array carries the single row of the
     * ApiVersions api itself, `18 0 4`, so that a client which guessed too high learns which version it should ask
     * for. A 2.8.2 broker answered `18 0 3` here, a 1.1.1 broker `00 00 00 00`.
     */
    public function testApiVersionsAnswersAnUnknownVersionWithTheErrorCode35(): void
    {
        $probe = new RawApiProbe(self::firstBootstrapServer());
        // The version the client asks for decides the header, so a v5 request carries the request header v2
        $result = $probe->send(ApiKeys::API_VERSIONS, 5, '', 3001, RawApiProbe::HEADER_V2);
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
            [ApiKeys::API_VERSIONS, 0, 4],
            array_values((array) unpack('n3', substr($result['body'], 6))),
            'KIP-511: the 35 names the version range of ApiVersions itself'
        );

        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(3002, $served['correlationId']);
    }

    /**
     * The flexible v3 of this api is what the client itself sends, and the node answers it with a header v0
     *
     * The probe builds the frame by hand as it does for every other key, which is what makes this an independent
     * check of {@see \Protocol\Kafka\Protocol\BinarySchema}: the same bytes the engine produces, assembled
     * without it, and the answer read without it as well. The tagged-field section at the end is where a KRaft
     * node differs from the ZooKeeper broker of the 2.x line, whose whole tail was `01 01 08 00 00 00 00 00 00 00
     * 00` - one tag, the epoch 0.
     */
    public function testTheFlexibleVersionOfApiVersionsIsAnsweredWithAResponseHeaderVersionZero(): void
    {
        $result = $this->probe(ApiKeys::API_VERSIONS, 3, 3004);
        $body   = $result['body'];

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(3004, $result['correlationId']);
        self::assertSame(
            0,
            self::errorCodeOf($body),
            'the error code follows the correlation id directly: no tag buffer, the response header stays v0'
        );
        self::assertSame(
            62,
            ord($body[2]),
            'the compact count of the api array is 61 + 1, in a single byte'
        );

        $tail = substr($body, self::taggedSectionOffset());
        self::assertSame(61, strlen($tail), 'three tagged fields of 23, 8 and 23 bytes, each behind a tag and a size');
        //  03                         three tagged fields
        //  00 17                      tag 0, supported_features, 23 bytes:
        //    02                         one feature, as the compact count 1 + 1
        //    11 metadata.version        its name, as the compact length 16 + 1 and the bytes
        //    00 01 00 15                min_version 1 (3.0-IV1), max_version 21 (3.9-IV0)
        //    00                         the tag buffer of the feature
        self::assertSame(
            '03' . '0017' . '02' . '11' . bin2hex(self::METADATA_VERSION_FEATURE) . '0001' . '0015' . '00',
            bin2hex(substr($tail, 0, 26))
        );
        //  01 08                      tag 1, finalized_features_epoch, 8 bytes: the int64 epoch
        self::assertSame('0108', bin2hex(substr($tail, 26, 2)));
        self::assertGreaterThanOrEqual(0, (int) unpack('J', substr($tail, 28, 8))[1], 'the epoch of a KRaft node');
        //  02 17                      tag 2, finalized_features, 23 bytes:
        //    02 11 metadata.version     one feature, its name
        //    00 15 00 15                max_version_level 21, min_version_level 21
        //    00                         the tag buffer of the feature - and the end of the body
        self::assertSame(
            '0217' . '02' . '11' . bin2hex(self::METADATA_VERSION_FEATURE) . '0015' . '0015' . '00',
            bin2hex(substr($tail, 36))
        );
    }

    /**
     * ApiVersions v4 (Kafka 3.9) is answered, and its one difference is a feature with a minimum of 0
     *
     * The specification @ 3.9.2 says why the version exists: "Version 4 fixes KAFKA-17011, which blocked
     * SupportedFeatures.MinVersion in the response from being 0". The answer to a v3 request omits every supported
     * feature whose minimum is 0 (`ApiVersionsResponse.Builder.maybeFilterSupportedFeatureKeys`), and on this node
     * that is `kraft.version` (KIP-853, supported 0 to 1, finalized at 0 because the quorum is the static
     * `controller.quorum.voters`): the v3 answer above lists one supported feature, the v4 answer lists two, and
     * the finalized features are the same in both because a feature finalized at level 0 is not finalized at all.
     * Everything in front of the tagged section - the api array and the throttle time - is byte for byte the v3
     * answer.
     */
    public function testApiVersionsVersionFourUnhidesTheSupportedFeaturesWithAMinimumOfZero(): void
    {
        $versionThree = $this->probe(ApiKeys::API_VERSIONS, 3, 3005)['body'];
        $result       = $this->probe(ApiKeys::API_VERSIONS, 4, 3006);
        $body         = $result['body'];

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(3006, $result['correlationId']);
        self::assertSame(
            substr($versionThree, 0, self::taggedSectionOffset()),
            substr($body, 0, self::taggedSectionOffset()),
            'the same 61 rows and the same throttle time'
        );

        $tail = substr($body, self::taggedSectionOffset());
        self::assertSame(80, strlen($tail), 'the supported-features tag grew from 23 to 42 bytes');
        //  03                         three tagged fields
        //  00 2a                      tag 0, supported_features, 42 bytes:
        //    03                         two features
        //    0e kraft.version           00 00 00 01  min_version 0, max_version 1  00
        //    11 metadata.version        00 01 00 15  min_version 1, max_version 21 00
        self::assertSame(
            '03' . '002a' . '03'
            . '0e' . bin2hex('kraft.version') . '0000' . '0001' . '00'
            . '11' . bin2hex(self::METADATA_VERSION_FEATURE) . '0001' . '0015' . '00',
            bin2hex(substr($tail, 0, 45))
        );
        self::assertSame('0108', bin2hex(substr($tail, 45, 2)), 'tag 1, the epoch');
        self::assertSame(
            '0217' . '02' . '11' . bin2hex(self::METADATA_VERSION_FEATURE) . '0015' . '0015' . '00',
            bin2hex(substr($tail, 55)),
            'tag 2, the finalized features, unchanged: kraft.version is finalized at 0, which is "not finalized"'
        );
    }

    /**
     * The header of an unknown ApiVersions version still has to be the one that version prescribes
     *
     * This is the trap KIP-482 set for a client that "just raises the version until the broker complains":
     * `RequestHeader.parse` @ 3.9.2 reads the api key and the api version first and asks
     * `ApiKeys.forId(apiKey).requestHeaderVersion(apiVersion)` how to read the rest, and for ApiVersions that
     * answer is **2** for every version above 2 - including the versions the node does not serve. A v5 frame with
     * the plain header of the versions 0 to 2 therefore ends in `Error parsing request header. Our best guess of
     * the apiKey is: 18` and a closed connection, and only the same frame with an empty tag buffer behind the
     * client id is answered with the 35 above.
     */
    public function testAnUnknownApiVersionsVersionWithTheOlderHeaderCostsTheConnection(): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::API_VERSIONS, 5, '', 3003, RawApiProbe::HEADER_V1);
        $probe->close();

        self::assertSame(RawApiProbe::CLOSED, $result['status'], 'the header v2 is not optional above v2');
    }

    /**
     * The flexible ApiVersions request has to name the client software, or the answer is 42
     *
     * `ApiVersionsRequest.isValid()` @ 3.9.2 matches both compact strings of v3 and v4 against
     * `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`, and `handleApiVersionsRequest` answers an invalid request with
     * **42** (InvalidRequest) in the layout of the requested version: an empty api array, the throttle time and an
     * empty tagged section. The connection stays open. The frame of the client carries
     * {@see ApiVersionsRequest::CLIENT_SOFTWARE_NAME} and {@see ApiVersionsRequest::CLIENT_SOFTWARE_VERSION} and
     * never sees it.
     */
    public function testApiVersionsRefusesAnEmptyClientSoftwareName(): void
    {
        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(
            ApiKeys::API_VERSIONS,
            3,
            RawApiProbe::compactString('') . RawApiProbe::compactString('') . RawApiProbe::tagBuffer(),
            3007,
            RawApiProbe::HEADER_V2
        );
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(3007, $result['correlationId']);
        //  00 2a          error_code 42
        //  01             [api_keys] = empty
        //  00 00 00 00    throttle_time_ms
        //  00             no tagged field
        self::assertSame('002a' . '01' . '00000000' . '00', bin2hex($result['body']));
    }

    /**
     * ControlledShutdown is not served on the client listener of a KRaft node, in none of its four versions
     *
     * Key 7 is the api that carries the whole history of the request header: **v0** has no client id at all
     * ({@see RawApiProbe::HEADER_V0}, the `"versions": "1+"` of the `ClientId` field in `RequestHeader.json`
     * @ 3.9.2), v1 and v2 use the common header, and **v3** - Kafka 2.4 - is flexible and uses the header v2. The
     * 2.x line verified that a ZooKeeper broker answers every one of them with the error 7 for a broker id it does
     * not know. The specification @ 3.9.2 lists the api for `["zkBroker", "controller"]`: a KRaft broker sends it
     * to the controller listener when it shuts down and never receives it, so on 9092 all four versions end in
     * `Received request api key CONTROLLED_SHUTDOWN with version 0 which is not enabled` and a closed connection -
     * the v0 frame with its header v0 included, because `RequestHeader.parse` still reads it correctly and it is
     * the listener check behind the parser that refuses it. Kafka 4.0.0 drops the specification altogether.
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
    public function testControlledShutdownIsNotServedOnTheClientListenerInAnyVersion(
        int $apiVersion,
        int $headerVersion
    ): void {
        $body = RawApiProbe::int32(self::UNKNOWN_BROKER_ID)
            . ($apiVersion >= 2 ? RawApiProbe::int64(-1) : '')
            . ($apiVersion >= 3 ? RawApiProbe::tagBuffer() : '');

        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send(ApiKeys::CONTROLLED_SHUTDOWN, $apiVersion, $body, 4200 + $apiVersion, $headerVersion);
        $probe->close();

        self::assertSame(
            RawApiProbe::CLOSED,
            $result['status'],
            "ControlledShutdown v{$apiVersion} belongs to the zkBroker and controller listeners"
        );
        self::assertNull($result['correlationId']);
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
     * connection - there is no error code for a frame the node cannot parse.
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

        $served = $this->probe(ApiKeys::METADATA, 12, 6002);

        self::assertSame(
            RawApiProbe::CLOSED,
            $closed['status'],
            'the api key 88 is above the table of Kafka 3.9.2'
        );
        self::assertSame(RawApiProbe::ANSWERED, $served['status']);
        self::assertSame(6002, $served['correlationId']);
    }

    /**
     * The answer of a flexible version is compact from the response header on
     *
     * The two apis a producer and a consumer live on became flexible in Kafka 2.8 and 2.7, and their empty answers
     * are the shortest illustration of what a flexible frame looks like: the **response header v1** is a
     * correlation id and a tagged-field section, every array counts `count + 1` in an unsigned varint, and every
     * structure ends in a tag buffer of its own. The empty Produce v11 answer is the same seven bytes as the v9
     * answer of the 2.x line, and the empty Fetch v17 answer the same thirteen as the v12 one: everything the
     * versions in between added to the two responses is either inside a partition - the `current_leader` of
     * KIP-951 - or a **tagged** `node_endpoints` array (tag 0 of Produce v10 and Fetch v16) that is only written
     * when it is not empty.
     */
    public function testTheAnswerOfAFlexibleVersionIsCompactFromTheResponseHeaderOn(): void
    {
        $produce = $this->probe(ApiKeys::PRODUCE, 11, 7001)['body'];
        $fetch   = $this->probe(ApiKeys::FETCH, 17, 7002)['body'];

        //  00                the tag buffer of the response header v1
        //  01                [responses] = empty, as the compact count 0 + 1
        //  00 00 00 00       throttle_time_ms
        //  00                the tag buffer of the body: no node_endpoints (tag 0) without a partition
        self::assertSame('00' . '01' . '00000000' . '00', bin2hex($produce));

        //  00                the tag buffer of the response header v1
        //  00 00 00 00       throttle_time_ms
        //  00 00             error_code (KIP-227)
        //  00 00 00 00       session_id
        //  01                [responses] = empty
        //  00                the tag buffer of the body: no node_endpoints (tag 0) either
        self::assertSame('00' . '00000000' . '0000' . '00000000' . '01' . '00', bin2hex($fetch));
    }

    /**
     * The ACL apis (29, 30, 31) are served by the `StandardAuthorizer` of the node and answer 0
     *
     * The ZooKeeper broker of the 2.x line ran without an `authorizer.class.name` and answered **54**
     * (SecurityDisabled) to every one of them. The container of this line runs the KRaft
     * `org.apache.kafka.metadata.authorizer.StandardAuthorizer` with `super.users=User:ANONYMOUS;...`,
     * so the anonymous principal of the PLAINTEXT listener passes every authorization and `handleDescribeAcls`
     * @ 3.9.2 answers the filter that matches everything - resource type, pattern type, operation and permission
     * type all ANY (1), every name null - with **0**, the error message at its default (the empty string, not
     * null) and the ACLs there are: none, because no test of this suite creates one. CreateAcls v3 and DeleteAcls
     * v3 with an empty array answer the same
     * `00 00 00 00 01 00` - throttle time, no result, tag buffer - after the broker forwarded them to the
     * controller. The error code 54 still exists in `Errors.java` @ 3.9.2, but no request of this suite can
     * provoke it any more.
     */
    public function testTheAclApisAreServedByTheStandardAuthorizer(): void
    {
        $result = $this->probe(ApiKeys::DESCRIBE_ACLS, 3, 9129);
        $body   = $this->responseBody($result['body'], ApiKeys::DESCRIBE_ACLS, 3);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        //  00 00 00 00    throttle_time_ms
        //  00 00          error_code 0
        //  01             error_message = "" (the default of the field, not null)
        //  01             [resources] = empty
        //  00             the tag buffer of the body
        self::assertSame(
            '00000000' . '0000' . '01' . '01' . '00',
            bin2hex($body),
            'ANONYMOUS is a super user of the StandardAuthorizer, and there is no acl to describe'
        );

        foreach ([ApiKeys::CREATE_ACLS, ApiKeys::DELETE_ACLS] as $apiKey) {
            $result = $this->probe($apiKey, 3, 9100 + $apiKey);

            self::assertSame(RawApiProbe::ANSWERED, $result['status']);
            self::assertSame(
                '00000000' . '01' . '00',
                bin2hex($this->responseBody($result['body'], $apiKey, 3)),
                "an empty array of acls to create or delete is answered with an empty array of results ({$apiKey})"
            );
        }
    }

    /**
     * SaslHandshake, the other api Kafka 0.10.0 added, is answered on the plaintext listener with 34
     *
     * On a SASL listener the request never reaches the api layer: `SaslServerAuthenticator` intercepts it while the
     * connection is still unauthenticated and answers it there. Everything that does get through to
     * `KafkaApis.handleSaslHandshakeRequest` @ 3.9.2 is therefore a handshake that arrived at the wrong moment,
     * and that method answers a constant **34** (IllegalSaslState) with the mechanisms the node has enabled - no
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
     * @ 3.9.2 is therefore always a request that came too late, and the method answers **34** with the message
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
     * KIP-48 lets a token be issued only over a channel that authenticated a real principal, and the KRaft node
     * did not change that: `KafkaApis.allowTokenRequests` @ 3.9.2 checks the security protocol of the listener
     * and the principal type first and answers **64** (`DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`) on PLAINTEXT and
     * on a one-way SSL channel, before the token manager is asked anything - Create and Describe at their v3 of
     * Kafka 3.3 (the owner principal a token can be created for), Renew and Expire at their v2. The container does
     * carry a `delegation.token.secret.key`, so the answer is not the 61 of a node without one.
     *
     * @return array<string, array{int, int}>
     */
    public static function delegationTokenApiProvider(): array
    {
        return [
            'CreateDelegationToken (38) v3'   => [ApiKeys::CREATE_DELEGATION_TOKEN, 3],
            'RenewDelegationToken (39) v2'    => [ApiKeys::RENEW_DELEGATION_TOKEN, 2],
            'ExpireDelegationToken (40) v2'   => [ApiKeys::EXPIRE_DELEGATION_TOKEN, 2],
            'DescribeDelegationToken (41) v3' => [ApiKeys::DESCRIBE_DELEGATION_TOKEN, 3],
        ];
    }

    #[DataProvider('delegationTokenApiProvider')]
    public function testTheDelegationTokenApisRefuseAPlaintextConnection(int $apiKey, int $apiVersion): void
    {
        $result = $this->probe($apiKey, $apiVersion, 9200 + $apiKey);
        $body   = $this->responseBody($result['body'], $apiKey, $apiVersion);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(
            KafkaException::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            self::errorCodeOf($body),
            'a delegation token is only issued over an authenticated channel'
        );
    }

    /**
     * UnregisterBroker (64) is forwarded to the controller, which refuses a broker id it never registered
     *
     * Kafka 2.8 (KIP-500) added the api for the controller listener; from 3.0 its specification lists `broker` as
     * well, and `KafkaApis` @ 3.9.2 dispatches it to `forwardToControllerOrFail` - a KRaft broker never handles
     * it itself. The controller answers **102** (BrokerIdNotRegistered) for {@see self::UNKNOWN_BROKER_ID} with
     * the error message at its default, the empty string, and nothing happens to node 1, which the Metadata v12
     * answer of the next connection still lists as the one broker of the cluster.
     */
    public function testUnregisterBrokerRefusesABrokerIdThatIsNotRegistered(): void
    {
        $result = $this->probe(ApiKeys::UNREGISTER_BROKER, 0, 9264);
        $body   = $this->responseBody($result['body'], ApiKeys::UNREGISTER_BROKER, 0);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        //  00 00 00 00    throttle_time_ms
        //  00 66          error_code 102
        //  01             error_message = "" (the default of the field, not null)
        //  00             the tag buffer of the body
        self::assertSame('00000000' . '0066' . '01' . '00', bin2hex($body));
        self::assertSame(KafkaException::BROKER_ID_NOT_REGISTERED, self::errorCodeBehindTheThrottleTimeOf($body));

        $metadata = $this->probe(ApiKeys::METADATA, 12, 9265);
        self::assertSame(RawApiProbe::ANSWERED, $metadata['status']);
        //  00 00 00 00    throttle_time_ms
        //  02             [brokers] = one broker
        //  00 00 00 01    node_id 1
        self::assertSame(
            '00000000' . '02' . '00000001',
            bin2hex(substr($this->responseBody($metadata['body'], ApiKeys::METADATA, 12), 0, 9)),
            'node 1 is still the one broker of the cluster'
        );
    }

    /**
     * DescribeQuorum (55) is forwarded to the controller and answered with no topic at all
     *
     * The api of KIP-595 became a client api in Kafka 3.0 (`listeners: ["broker", "controller"]`), and a request
     * that names no partition is answered with an empty topic array: the v2 of Kafka 3.9 (KIP-853) adds an error
     * message - at its default, the empty string - and the `nodes` array of the voters' endpoints, which is only
     * filled for the partitions that were asked for. Asking for the `__cluster_metadata` partition itself belongs
     * to the ticket that implements the api.
     */
    public function testDescribeQuorumIsForwardedToTheControllerAndAnswersAnEmptyTopicArray(): void
    {
        $result = $this->probe(ApiKeys::DESCRIBE_QUORUM, 2, 9255);
        $body   = $this->responseBody($result['body'], ApiKeys::DESCRIBE_QUORUM, 2);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        //  00 00          error_code 0
        //  01             error_message = "" (the default of the field, not null)
        //  01             [topics] = empty
        //  01             [nodes] = empty
        //  00             the tag buffer of the body
        self::assertSame('0000' . '01' . '01' . '01' . '00', bin2hex($body));
    }

    /**
     * ConsumerGroupHeartbeat (68) of an unknown group answers 69 and creates nothing
     *
     * The heartbeat of the KIP-848 consumer protocol (its specification exists since the tag 3.5.2; the container
     * serves it because of its `group.coordinator.rebalance.protocols=classic,consumer`) is the one request that
     * *creates* a consumer
     * group: a member epoch of 0 with an empty member id is a join, and the coordinator would create
     * {@see self::$groupId} on the spot. The probe sends the member epoch **-1** instead - a leave - for a member
     * of a group that does not exist, which `GroupMetadataManager.consumerGroupLeave` @ 3.9.2 answers with **69**
     * (GroupIdNotFound) and the message `Group <id> not found.`; a positive epoch is refused the same way with
     * `Consumer group <id> not found.`. The rest of the answer is at its defaults: a null member id, epoch 0,
     * heartbeat interval 0 and a null assignment. ListGroups v5 afterwards does not know the group, and
     * ConsumerGroupDescribe (69) v0 of the same group id answers 69 for it.
     */
    public function testConsumerGroupHeartbeatOfAnUnknownGroupAnswersGroupIdNotFoundAndCreatesNothing(): void
    {
        $result = $this->probe(ApiKeys::CONSUMER_GROUP_HEARTBEAT, 0, 9268);
        $body   = $this->responseBody($result['body'], ApiKeys::CONSUMER_GROUP_HEARTBEAT, 0);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(0, self::throttleTimeOf($body));
        self::assertSame(KafkaException::GROUP_ID_NOT_FOUND, self::errorCodeBehindTheThrottleTimeOf($body));
        $message = "Group {$this->groupId} not found.";
        self::assertSame(
            RawApiProbe::compactString($message)
                . RawApiProbe::compactString(null)       // member_id
                . RawApiProbe::int32(0)                  // member_epoch
                . RawApiProbe::int32(0)                  // heartbeat_interval_ms
                . RawApiProbe::nullStruct()              // assignment
                . RawApiProbe::tagBuffer(),
            substr($body, 6)
        );

        $groups = $this->probe(ApiKeys::LIST_GROUPS, 5, 9269);
        self::assertSame(RawApiProbe::ANSWERED, $groups['status']);
        self::assertStringNotContainsString($this->groupId, $groups['body'], 'the heartbeat created no group');

        $probe    = new RawApiProbe(self::firstBootstrapServer());
        $describe = $probe->send(
            ApiKeys::CONSUMER_GROUP_DESCRIBE,
            0,
            RawApiProbe::compactArray(1) . RawApiProbe::compactString($this->groupId) . RawApiProbe::boolean(false)
                . RawApiProbe::tagBuffer(),
            9270,
            RawApiProbe::HEADER_V2
        );
        $probe->close();

        self::assertSame(RawApiProbe::ANSWERED, $describe['status']);
        $body = $this->responseBody($describe['body'], ApiKeys::CONSUMER_GROUP_DESCRIBE, 0);
        //  00 00 00 00    throttle_time_ms
        //  02             [groups] = one group
        //  00 45          its error_code 69
        self::assertSame('00000000' . '02' . '0045', bin2hex(substr($body, 0, 7)));
    }

    /**
     * DescribeTopicPartitions (75) of a topic that does not exist answers 3 for the topic and no cursor
     *
     * The api of Kafka 3.8 is a paginated Metadata: an **empty** topic array asks for *every* topic
     * of the cluster, the way Metadata v0 did, so the probe names {@see self::$missingTopic} instead and gets the
     * error **3** (UnknownTopicOrPartition) in the row of that topic, an all-zero topic id, no partition and the
     * authorized operations the super user has on a topic that does not exist; the `next_cursor` of the answer
     * is null because there is no next page. Nothing is auto-created: only Metadata with
     * `allow_auto_topic_creation` does that.
     */
    public function testDescribeTopicPartitionsOfAnUnknownTopicAnswersUnknownTopicAndNoCursor(): void
    {
        $result = $this->probe(ApiKeys::DESCRIBE_TOPIC_PARTITIONS, 0, 9275);
        $body   = $this->responseBody($result['body'], ApiKeys::DESCRIBE_TOPIC_PARTITIONS, 0);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        //  00 00 00 00    throttle_time_ms
        //  02             [topics] = one topic
        //  00 03          its error_code 3
        //  <name>         its name, as a compact string
        //  00 * 16        topic_id = all zero
        //  00             is_internal = false
        //  01             [partitions] = empty
        self::assertStringStartsWith(
            '00000000' . '02' . '0003' . bin2hex(RawApiProbe::compactString($this->missingTopic))
            . str_repeat('00', 16) . '00' . '01',
            bin2hex($body)
        );
        self::assertStringEndsWith(
            bin2hex(RawApiProbe::tagBuffer() . RawApiProbe::nullStruct() . RawApiProbe::tagBuffer()),
            bin2hex($body),
            'the tag buffer of the topic, a null next_cursor and the tag buffer of the body'
        );
    }

    /**
     * AddRaftVoter (80) and RemoveRaftVoter (81) refuse a frame of another cluster before they look at the voter
     *
     * The two reconfiguration apis of KIP-853 (Kafka 3.9) are the only apis of the table that could change the
     * controller quorum itself, so the probe sends them with {@see self::FOREIGN_CLUSTER_ID}:
     * `KafkaRaftClient.handleAddVoterRequest` @ 3.9.2 checks `hasValidClusterId` first and answers **104**
     * (InconsistentClusterId) with `The given id "not-this-cluster" doesn't match the cluster id "<id>"`, and
     * the same for the removal. With the real cluster id - or a null one, which the check accepts - the same
     * frame is refused one step later with **42** (InvalidRequest) and `Add voter request didn't include a
     * valid voter`, because the all-zero directory id is not a directory id; and a frame that names a *valid*
     * voter never reaches the quorum either, see
     * {@see self::testTheRaftVoterApisAreRefusedBecauseTheKraftVersionFeatureIsZero()}. The quorum of the
     * container stays `[{id: 1, endpoints: [CONTROLLER://localhost:9096]}]` in every case.
     *
     * @return array<string, array{int}>
     */
    public static function raftVoterApiProvider(): array
    {
        return [
            'AddRaftVoter (80)'    => [ApiKeys::ADD_RAFT_VOTER],
            'RemoveRaftVoter (81)' => [ApiKeys::REMOVE_RAFT_VOTER],
        ];
    }

    #[DataProvider('raftVoterApiProvider')]
    public function testTheRaftVoterApisRefuseAForeignClusterId(int $apiKey): void
    {
        $result = $this->probe($apiKey, 0, 9200 + $apiKey);
        $body   = $this->responseBody($result['body'], $apiKey, 0);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(0, self::throttleTimeOf($body));
        self::assertSame(KafkaException::INCONSISTENT_CLUSTER_ID, self::errorCodeBehindTheThrottleTimeOf($body));
        self::assertStringContainsString(
            'The given id "' . self::FOREIGN_CLUSTER_ID . '" doesn\'t match the cluster id "',
            $body
        );
    }

    /**
     * A well-formed AddRaftVoter or RemoveRaftVoter is refused **35**, and the three voter codes stay unreachable
     *
     * This is the measurement the error codes 125 to 127 (`InvalidVoterKey`, `DuplicateVoter`, `VoterNotFound`)
     * hang on, and the answer is that a 3.9.2 node with the default configuration never writes one of them. The
     * reconfiguration of KIP-853 is gated on the **`kraft.version` feature**: the node supports 0 to 1 and has
     * finalized **0**, which is the static `controller.quorum.voters` of KIP-595, and `KafkaRaftClient` refuses
     * every reconfiguration of such a quorum before it compares a voter at all:
     *
     * * with the **foreign** cluster id, the **104** of the test above - the cluster id is checked first;
     * * with the real cluster id (or a **null** one, which `hasValidClusterId` accepts) and a voter whose
     *   directory id is the zero uuid or whose listener array is empty, **42** (`InvalidRequest`) and
     *   `Add voter request didn't include a valid voter` / `Remove voter request didn't include a valid voter`;
     * * with the real cluster id and a **valid** voter key - a random directory id and one endpoint - **35**
     *   (`UnsupportedVersion`) and `Cluster doesn't support adding voter because the kraft.version feature is 0`,
     *   the removal saying `removing voter` in the same sentence.
     *
     * The voter named here is {@see self::UNKNOWN_BROKER_ID} with a directory id drawn at random, so the frame
     * could not describe a node of this cluster even if the feature allowed it; the request changes nothing.
     */
    #[DataProvider('raftVoterApiProvider')]
    public function testTheRaftVoterApisAreRefusedBecauseTheKraftVersionFeatureIsZero(int $apiKey): void
    {
        $clusterId  = $this->admin()->describeCluster()->clusterId;
        $verb       = $apiKey === ApiKeys::ADD_RAFT_VOTER ? 'adding' : 'removing';
        $directory  = random_bytes(16);
        $wellFormed = $apiKey === ApiKeys::ADD_RAFT_VOTER
            ? RawApiProbe::compactString($clusterId) . RawApiProbe::int32(1000)
                . RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . $directory
                . RawApiProbe::compactArray(1) . RawApiProbe::compactString('CONTROLLER')
                . RawApiProbe::compactString('localhost') . pack('n', 9097) . RawApiProbe::tagBuffer()
                . RawApiProbe::tagBuffer()
            : RawApiProbe::compactString($clusterId) . RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . $directory
                . RawApiProbe::tagBuffer();

        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send($apiKey, 0, $wellFormed, 9300 + $apiKey, RawApiProbe::HEADER_V2);
        $probe->close();

        $body = $this->responseBody($result['body'], $apiKey, 0);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(0, self::throttleTimeOf($body));
        self::assertSame(
            KafkaException::UNSUPPORTED_VERSION,
            self::errorCodeBehindTheThrottleTimeOf($body),
            'the reconfiguration of KIP-853 needs the kraft.version feature at the level 1'
        );
        self::assertStringContainsString(
            "Cluster doesn't support {$verb} voter because the kraft.version feature is 0",
            $body,
            'the codes 125 to 127 are never reached on a quorum with a static voter set'
        );
    }

    /**
     * A voter key the api cannot read is the 42, and it is checked before the feature
     *
     * The zero directory id and the empty listener array of {@see self::body()} are what `VoterSet.VoterNode`
     * @ 3.9.2 refuses to build, so `handleAddVoterRequest` answers `Add voter request didn't include a valid
     * voter` before it asks whether the quorum could be reconfigured at all.
     */
    #[DataProvider('raftVoterApiProvider')]
    public function testAVoterKeyTheApiCannotReadIsTheFortyTwo(int $apiKey): void
    {
        $clusterId = $this->admin()->describeCluster()->clusterId;
        $verb      = $apiKey === ApiKeys::ADD_RAFT_VOTER ? 'Add' : 'Remove';
        $zeroUuid  = str_repeat("\x00", 16);
        $body      = $apiKey === ApiKeys::ADD_RAFT_VOTER
            ? RawApiProbe::compactString($clusterId) . RawApiProbe::int32(1000)
                . RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . $zeroUuid
                . RawApiProbe::compactArray(0) . RawApiProbe::tagBuffer()
            : RawApiProbe::compactString($clusterId) . RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . $zeroUuid
                . RawApiProbe::tagBuffer();

        $probe  = new RawApiProbe(self::firstBootstrapServer());
        $result = $probe->send($apiKey, 0, $body, 9400 + $apiKey, RawApiProbe::HEADER_V2);
        $probe->close();

        $answer = $this->responseBody($result['body'], $apiKey, 0);

        self::assertSame(RawApiProbe::ANSWERED, $result['status']);
        self::assertSame(KafkaException::INVALID_REQUEST, self::errorCodeBehindTheThrottleTimeOf($answer));
        self::assertStringContainsString("{$verb} voter request didn't include a valid voter", $answer);
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
     * `ApiKeys.requestHeaderVersion()` @ 3.9.2: the header v2 from the first flexible version of the api on, and
     * the common header v1 everywhere else. The node derives it from the version of the frame it is reading,
     * served or not, so this holds for the versions above the table as well. The header v0 of ControlledShutdown
     * v0 is not needed here any more, because key 7 is not in the table of the client listener.
     */
    private static function headerVersionOf(int $apiKey, int $apiVersion): int
    {
        return ($apiVersion >= (self::FLEXIBLE_FROM[$apiKey] ?? PHP_INT_MAX))
            ? RawApiProbe::HEADER_V2
            : RawApiProbe::HEADER_V1;
    }

    /**
     * Strips the tagged-field section that the **response header v1** of a flexible answer carries
     *
     * `ApiKeys.responseHeaderVersion()` @ 3.9.2 answers 1 for every flexible version, so the body of such an answer
     * starts with the empty tag buffer of its header - one `00` byte in front of everything the api documents.
     * ApiVersions is the exception the generator writes out by hand: its answer always carries a response header
     * v0, so that a client can read the 35 of an older broker. The two telemetry apis are not in the table and
     * therefore not in {@see self::FLEXIBLE_FROM}, but their only version is flexible and their answers carry the
     * header v1 like every other flexible answer.
     */
    private function responseBody(string $body, int $apiKey, int $apiVersion): string
    {
        $flexibleFrom = self::FLEXIBLE_FROM[$apiKey]
            ?? (in_array($apiKey, self::HIDDEN_TELEMETRY_KEYS, true) ? 0 : PHP_INT_MAX);
        if ($apiKey === ApiKeys::API_VERSIONS || $apiVersion < $flexibleFrom) {
            return $body;
        }

        self::assertSame("\x00", substr($body, 0, 1), 'the response header v1 ends in an empty tag buffer');

        return substr($body, 1);
    }

    /**
     * Returns the offset of the tagged-field section in a flexible ApiVersions answer of this node
     *
     * The error code, the compact count of the api array, seven bytes per row (three int16 and the tag buffer of
     * the row) and the throttle time: `2 + 1 + 61 * 7 + 4`.
     */
    private static function taggedSectionOffset(): int
    {
        return 2 + 1 + count(self::SERVED_APIS) * 7 + 4;
    }

    /**
     * Builds the smallest well-formed body of the **maximum** version of the given api that changes nothing
     *
     * Every api of the table is here, and the ones whose maximum version is flexible are built with the compact
     * types of KIP-482: `RawApiProbe::compactArray(0)` is the empty array (`01`), `compactString(null)` the null
     * string (`00`), and every structure - the body included - ends in an empty `tagBuffer()`.
     *
     * Two apis of this node **never answer an empty array**, and are the reason the rule "an empty array is the
     * smallest body" has two exceptions here. OffsetFetch v9 with no group runs its handler to the end, but
     * `RequestChannel.sendResponse` then dies in `RequestMetrics.markErrorMeter` with
     * `NoSuchElementException: key not found: null` - the error counts of an answer without a group carry a null
     * key - and the request is dropped without a frame (`Unexpected error handling request` in the log, the
     * connection stays open). AddPartitionsToTxn v5 with no transaction never sends at all:
     * `addResultAndMaybeSendResponse` @ 3.9.2 sends when `responses.size == txns.size`, and with zero
     * transactions nothing ever calls it. Both are sent with **one** harmless element instead.
     */
    private function body(int $apiKey, int $apiVersion): string
    {
        $emptyArray      = RawApiProbe::compactArray(0);
        $nullString      = RawApiProbe::compactString(null);
        $tag             = RawApiProbe::tagBuffer();
        $zeroUuid        = str_repeat("\x00", 16);
        // No producer of the cluster owns this id, so every transaction api answers an error and changes nothing
        $unknownProducer = RawApiProbe::int64(-1) . RawApiProbe::int16(-1);

        return match ($apiKey) {
            // A null transactional id, acks 1, a timeout and no topic at all; v10 (KIP-951) and v11 (KIP-890)
            // changed the response and the error codes, not the request
            ApiKeys::PRODUCE => $nullString . RawApiProbe::int16(1) . RawApiProbe::int32(1000) . $emptyArray . $tag,
            // The replica id left the body with v15 (KIP-903: a follower sends the tagged `replica_state`, a
            // consumer sends nothing), and the cluster id (tag 0 of v12) and the replica directory id (tag 0 of a
            // partition, v17, KIP-853) are tagged as well, so a consumer's frame is: the session of KIP-227 (id 0
            // and epoch -1 are the session-less full fetch), no topic, no forgotten topic and the empty rack id of
            // KIP-392
            ApiKeys::FETCH => RawApiProbe::int32(100) . RawApiProbe::int32(0) . RawApiProbe::int32(1048576)
                . RawApiProbe::int8(0) . RawApiProbe::int32(0) . RawApiProbe::int32(-1)
                . $emptyArray . $emptyArray . RawApiProbe::compactString('') . $tag,
            // v7 (KIP-734), v8 (KIP-405) and v9 (KIP-1005) added timestamps a partition can ask for, not fields
            ApiKeys::OFFSETS => RawApiProbe::int32(-1) . RawApiProbe::int8(0) . $emptyArray . $tag,
            // An empty topic array is "no topic" since v1; `allow_auto_topic_creation` is false so that the probe
            // creates nothing, the cluster authorized operations of KIP-430 left the body with v11 and the topic
            // authorized operations are not asked for; v12 (Kafka 3.1) made the topic id of a request usable
            ApiKeys::METADATA => $emptyArray . RawApiProbe::boolean(false) . RawApiProbe::boolean(false) . $tag,
            // v9 (KIP-848) renamed the generation id to `generation_id_or_member_epoch`, with the same bytes
            ApiKeys::OFFSET_COMMIT => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('') . $nullString . $emptyArray . $tag,
            // One group (v8, Kafka 3.0, batched them) with a null member id and the member epoch -1 of v9
            // (KIP-848) and an empty topic array - "no partition" where the null array would mean "every
            // partition"; the answer is the group with no topic and the error 0, and no group is created
            ApiKeys::OFFSET_FETCH => RawApiProbe::compactArray(1)
                . RawApiProbe::compactString($this->groupId) . $nullString . RawApiProbe::int32(-1) . $emptyArray
                . $tag
                . RawApiProbe::boolean(false) . $tag,
            // The single key of v0 to v3 left the body with v4 (KIP-699), which asks for a batch of coordinator
            // keys instead: key type 0 is a group, and no key is asked for. v5 (KIP-890) and v6 (KIP-932, the
            // share-group key type 2) added nothing to the body
            ApiKeys::GROUP_COORDINATOR => RawApiProbe::int8(0) . $emptyArray . $tag,
            // A session timeout of 1 ms is below group.min.session.timeout.ms, so no group is created; v8 added
            // the null reason of KIP-800, v9 is the same as v8
            ApiKeys::JOIN_GROUP => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(1)
                . RawApiProbe::int32(1) . RawApiProbe::compactString('') . $nullString
                . RawApiProbe::compactString('consumer') . $emptyArray . $nullString . $tag,
            ApiKeys::HEARTBEAT => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('probe-member') . $nullString . $tag,
            // KIP-345 turned the single member id of v0-v2 into a batch of member identities; v5 gave each of them
            // the null reason of KIP-800, and the batch is empty here
            ApiKeys::LEAVE_GROUP => RawApiProbe::compactString($this->groupId) . $emptyArray . $tag,
            ApiKeys::SYNC_GROUP => RawApiProbe::compactString($this->groupId) . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('probe-member') . $nullString . $nullString . $nullString
                . $emptyArray . $tag,
            ApiKeys::DESCRIBE_GROUPS => $emptyArray . RawApiProbe::boolean(false) . $tag,
            // KIP-518 added the states filter and v5 (KIP-848) the types filter; both empty here: every group
            ApiKeys::LIST_GROUPS => $emptyArray . $emptyArray . $tag,
            // A mechanism the listener does not offer, so the node answers 34 and creates nothing
            ApiKeys::SASL_HANDSHAKE => RawApiProbe::string('KAFKA-CLIENT-PROBE'),
            // The versions 0 to 2 have no body at all; v3 (KIP-511) carries the name and the version of the client
            // software as compact strings, which the node matches against `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`,
            // and v4 (KAFKA-17011) is the same body
            ApiKeys::API_VERSIONS => $apiVersion >= 3
                ? RawApiProbe::compactString(ApiVersionsRequest::CLIENT_SOFTWARE_NAME)
                    . RawApiProbe::compactString(ApiVersionsRequest::CLIENT_SOFTWARE_VERSION) . $tag
                : '',
            // No topic to create and no topic to delete
            ApiKeys::CREATE_TOPICS => $emptyArray . RawApiProbe::int32(1000) . RawApiProbe::boolean(true) . $tag,
            ApiKeys::DELETE_TOPICS => $emptyArray . RawApiProbe::int32(1000) . $tag,
            // No log to truncate and no partition to look an epoch up in
            ApiKeys::DELETE_RECORDS => $emptyArray . RawApiProbe::int32(1000) . $tag,
            // -2 is the replica id of an ordinary consumer (KIP-320), the default of the field
            ApiKeys::OFFSET_FOR_LEADER_EPOCH => RawApiProbe::int32(-2) . $emptyArray . $tag,
            // A null transactional id asks for a bare producer id, which allocates one and creates no transaction;
            // v5 (KIP-890) added an error code, not a field
            ApiKeys::INIT_PRODUCER_ID => $nullString . RawApiProbe::int32(1000) . RawApiProbe::int64(-1)
                . RawApiProbe::int16(-1) . $tag,
            // v4 batched the transactions and gave each a `verify_only` flag, and the specification says "versions
            // 4 and above will be used by brokers" - the frame needs the CLUSTER_ACTION authorization, which the
            // super user of the container has. One transaction with the id the coordinator has never seen, the
            // producer id -1, verify only and no topic: the answer is that transaction with no topic result
            ApiKeys::ADD_PARTITIONS_TO_TXN => RawApiProbe::compactArray(1)
                . RawApiProbe::compactString($this->transactionalId) . $unknownProducer . RawApiProbe::boolean(true)
                . $emptyArray . $tag
                . $tag,
            // Every other transaction api names a transactional id the coordinator has never seen, with the
            // producer id -1: the answer is an error, and no transaction state is written. v4 of AddOffsetsToTxn,
            // EndTxn and TxnOffsetCommit (KIP-890, Kafka 3.8) added the error code 120, not a field
            ApiKeys::ADD_OFFSETS_TO_TXN => RawApiProbe::compactString($this->transactionalId) . $unknownProducer
                . RawApiProbe::compactString($this->groupId) . $tag,
            ApiKeys::END_TXN => RawApiProbe::compactString($this->transactionalId) . $unknownProducer
                . RawApiProbe::boolean(false) . $tag,
            ApiKeys::WRITE_TXN_MARKERS => $emptyArray . $tag,
            ApiKeys::TXN_OFFSET_COMMIT => RawApiProbe::compactString($this->transactionalId)
                . RawApiProbe::compactString($this->groupId) . $unknownProducer . RawApiProbe::int32(-1)
                . RawApiProbe::compactString('') . $nullString . $emptyArray . $tag,
            // A filter that matches everything, on a node whose StandardAuthorizer holds no acl; 1 is ANY for the
            // resource type, the pattern type of KIP-290, the operation and the permission type alike. v3 of the
            // three acl apis (Kafka 3.3) added the resource type USER of the token owners, not a field
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
            // null array of DescribeLogDirs would ask for every partition of both log directories of the container;
            // v3 (Kafka 3.2, a top-level error code) and v4 (Kafka 3.3, the total and usable bytes of a log
            // directory) of DescribeLogDirs changed the response only
            ApiKeys::ALTER_REPLICA_LOG_DIRS => $emptyArray . $tag,
            ApiKeys::DESCRIBE_LOG_DIRS      => $emptyArray . $tag,
            // An empty token: the api is answered with 34 on every listener that did not authenticate with it
            ApiKeys::SASL_AUTHENTICATE => RawApiProbe::compactBytes('') . $tag,
            // No topic whose partition count should grow, and `validate_only` on top of that
            ApiKeys::CREATE_PARTITIONS => $emptyArray . RawApiProbe::int32(1000) . RawApiProbe::boolean(true) . $tag,
            // The token apis are refused on a PLAINTEXT connection before anything is read out of their body, so
            // the smallest well-formed frame of each is enough: v3 of Create (Kafka 3.3) opens with the
            // null owner principal type and name, then no renewer and no lifetime; no hmac; no owner
            ApiKeys::CREATE_DELEGATION_TOKEN   => $nullString . $nullString . $emptyArray . RawApiProbe::int64(-1)
                . $tag,
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
            // No partition of the raft log to describe (KIP-595); v1 (KIP-836) and v2 (KIP-853) grew the response
            ApiKeys::DESCRIBE_QUORUM => $emptyArray . $tag,
            // No feature to update (KIP-584), and v1 (Kafka 3.3) adds `validate_only` on top of that
            ApiKeys::UPDATE_FEATURES => RawApiProbe::int32(1000) . $emptyArray . RawApiProbe::boolean(true) . $tag,
            // Nothing to authorize (KIP-700); the endpoint type 1 of v1 (KIP-919, Kafka 3.7) asks for the brokers,
            // and 2 - the controllers - is refused on this listener with 114 (MismatchedEndpointType)
            ApiKeys::DESCRIBE_CLUSTER => RawApiProbe::boolean(false) . RawApiProbe::int8(1) . $tag,
            // No topic to describe (KIP-664)
            ApiKeys::DESCRIBE_PRODUCERS => $emptyArray . $tag,
            // A broker id no node of the cluster has, so the controller answers 102 and unregisters nothing
            ApiKeys::UNREGISTER_BROKER => RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . $tag,
            // No transactional id to describe (KIP-664), and a ListTransactions that filters nothing: no state, no
            // producer id and the `duration_filter` -1 of v1 (KIP-994, Kafka 3.8) that means "any duration"
            ApiKeys::DESCRIBE_TRANSACTIONS => $emptyArray . $tag,
            ApiKeys::LIST_TRANSACTIONS     => $emptyArray . $emptyArray . RawApiProbe::int64(-1) . $tag,
            // A leave (member epoch -1) of a member of a group that does not exist: 69, and no group is created
            // (KIP-848). Everything else at its default: null instance and rack id, rebalance timeout -1, null
            // subscription, null assignor, null partitions
            ApiKeys::CONSUMER_GROUP_HEARTBEAT => RawApiProbe::compactString($this->groupId)
                . RawApiProbe::compactString('probe-member') . RawApiProbe::int32(-1) . $nullString . $nullString
                . RawApiProbe::int32(-1) . $nullString . $nullString . $nullString . $tag,
            // No group to describe (KIP-848), and no authorized operations asked for
            ApiKeys::CONSUMER_GROUP_DESCRIBE => $emptyArray . RawApiProbe::boolean(false) . $tag,
            // The one api of the table without a field (KIP-714): a tag buffer is its whole body
            ApiKeys::LIST_CLIENT_METRICS_RESOURCES => $tag,
            // One topic that does not exist - the empty array would describe every topic of the cluster - the
            // default page size 2000 and a null cursor (Kafka 3.8)
            ApiKeys::DESCRIBE_TOPIC_PARTITIONS => RawApiProbe::compactArray(1)
                . RawApiProbe::compactString($this->missingTopic) . $tag
                . RawApiProbe::int32(2000) . RawApiProbe::nullStruct() . $tag,
            // The cluster id of another cluster, so that the controller answers 104 before it reads the voter: a
            // timeout of 1 ms, the voter id nobody has, the all-zero directory id and no listener (KIP-853)
            ApiKeys::ADD_RAFT_VOTER => RawApiProbe::compactString(self::FOREIGN_CLUSTER_ID) . RawApiProbe::int32(1)
                . RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . $zeroUuid . $emptyArray . $tag,
            ApiKeys::REMOVE_RAFT_VOTER => RawApiProbe::compactString(self::FOREIGN_CLUSTER_ID)
                . RawApiProbe::int32(self::UNKNOWN_BROKER_ID) . $zeroUuid . $tag,
            default => '',
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
