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

namespace Protocol\Kafka\Tests\Unit\Protocol;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Protocol\ApiKeys;
use ReflectionClass;

/**
 * Verifies that the api keys of this branch are exactly the ones of Kafka 2.8.2.
 *
 * The list mirrors `org.apache.kafka.common.protocol.ApiKeys` @ 2.8.2, which ends at key 64. The names are the
 * ones of the pre-schema `main` branch wherever the concept exists there: key 10 is GroupCoordinator here, although
 * 0.11 renamed the api to FindCoordinator and the 0.8.2 sources called it ConsumerMetadata; the keys 34 to 42 carry
 * the names of the Java client.
 *
 * @see \Protocol\Kafka\Tests\Integration\ApiVersionProbeTest for the same list verified against a real broker
 */
#[CoversClass(ApiKeys::class)]
final class ApiKeysTest extends TestCase
{
    /**
     * Every api key of Kafka 2.8.2, in the order of org.apache.kafka.common.protocol.ApiKeys
     *
     * @var array<string, int>
     */
    private const array KEYS_OF_KAFKA_3_9_2 = [
        'PRODUCE'                 => 0,
        'FETCH'                   => 1,
        'OFFSETS'                 => 2,
        'METADATA'                => 3,
        'LEADER_AND_ISR'          => 4,
        'STOP_REPLICA'            => 5,
        'UPDATE_METADATA'         => 6,
        'CONTROLLED_SHUTDOWN'     => 7,
        'OFFSET_COMMIT'           => 8,
        'OFFSET_FETCH'            => 9,
        'GROUP_COORDINATOR'       => 10,
        'JOIN_GROUP'              => 11,
        'HEARTBEAT'               => 12,
        'LEAVE_GROUP'             => 13,
        'SYNC_GROUP'              => 14,
        'DESCRIBE_GROUPS'         => 15,
        'LIST_GROUPS'             => 16,
        'SASL_HANDSHAKE'          => 17,
        'API_VERSIONS'            => 18,
        'CREATE_TOPICS'           => 19,
        'DELETE_TOPICS'           => 20,
        'DELETE_RECORDS'          => 21,
        'INIT_PRODUCER_ID'        => 22,
        'OFFSET_FOR_LEADER_EPOCH' => 23,
        'ADD_PARTITIONS_TO_TXN'   => 24,
        'ADD_OFFSETS_TO_TXN'      => 25,
        'END_TXN'                 => 26,
        'WRITE_TXN_MARKERS'       => 27,
        'TXN_OFFSET_COMMIT'       => 28,
        'DESCRIBE_ACLS'           => 29,
        'CREATE_ACLS'             => 30,
        'DELETE_ACLS'             => 31,
        'DESCRIBE_CONFIGS'        => 32,
        'ALTER_CONFIGS'           => 33,
        'ALTER_REPLICA_LOG_DIRS'  => 34,
        'DESCRIBE_LOG_DIRS'       => 35,
        'SASL_AUTHENTICATE'       => 36,
        'CREATE_PARTITIONS'       => 37,
        'CREATE_DELEGATION_TOKEN'   => 38,
        'RENEW_DELEGATION_TOKEN'    => 39,
        'EXPIRE_DELEGATION_TOKEN'   => 40,
        'DESCRIBE_DELEGATION_TOKEN' => 41,
        'DELETE_GROUPS'             => 42,
        'ELECT_LEADERS'                   => 43,
        'INCREMENTAL_ALTER_CONFIGS'       => 44,
        'ALTER_PARTITION_REASSIGNMENTS'   => 45,
        'LIST_PARTITION_REASSIGNMENTS'    => 46,
        'OFFSET_DELETE'                   => 47,
        'DESCRIBE_CLIENT_QUOTAS'          => 48,
        'ALTER_CLIENT_QUOTAS'             => 49,
        'DESCRIBE_USER_SCRAM_CREDENTIALS' => 50,
        'ALTER_USER_SCRAM_CREDENTIALS'    => 51,
        'VOTE'                            => 52,
        'BEGIN_QUORUM_EPOCH'              => 53,
        'END_QUORUM_EPOCH'                => 54,
        'DESCRIBE_QUORUM'                 => 55,
        'ALTER_ISR'                       => 56,
        'UPDATE_FEATURES'                 => 57,
        'ENVELOPE'                        => 58,
        'FETCH_SNAPSHOT'                  => 59,
        'DESCRIBE_CLUSTER'                => 60,
        'DESCRIBE_PRODUCERS'              => 61,
        'BROKER_REGISTRATION'             => 62,
        'BROKER_HEARTBEAT'                => 63,
        'UNREGISTER_BROKER'               => 64,
        'DESCRIBE_TRANSACTIONS'           => 65,
        'LIST_TRANSACTIONS'               => 66,
        'ALLOCATE_PRODUCER_IDS'           => 67,
        'CONSUMER_GROUP_HEARTBEAT'        => 68,
        'CONSUMER_GROUP_DESCRIBE'         => 69,
        'CONTROLLER_REGISTRATION'         => 70,
        'GET_TELEMETRY_SUBSCRIPTIONS'     => 71,
        'PUSH_TELEMETRY'                  => 72,
        'ASSIGN_REPLICAS_TO_DIRS'         => 73,
        'LIST_CLIENT_METRICS_RESOURCES'   => 74,
        'DESCRIBE_TOPIC_PARTITIONS'       => 75,
        'SHARE_GROUP_HEARTBEAT'           => 76,
        'SHARE_GROUP_DESCRIBE'            => 77,
        'SHARE_FETCH'                     => 78,
        'SHARE_ACKNOWLEDGE'               => 79,
        'ADD_RAFT_VOTER'                  => 80,
        'REMOVE_RAFT_VOTER'               => 81,
        'UPDATE_RAFT_VOTER'               => 82,
        'INITIALIZE_SHARE_GROUP_STATE'    => 83,
        'READ_SHARE_GROUP_STATE'          => 84,
        'WRITE_SHARE_GROUP_STATE'         => 85,
        'DELETE_SHARE_GROUP_STATE'        => 86,
        'READ_SHARE_GROUP_STATE_SUMMARY'  => 87,
    ];

    public function testTheApiKeysAreExactlyTheOnesOfKafka392(): void
    {
        self::assertSame(
            self::KEYS_OF_KAFKA_3_9_2,
            new ReflectionClass(ApiKeys::class)->getConstants(),
            'The branch declares an api key that Kafka 3.9.2 does not have, or misses one that it has'
        );
    }

    public function testTheGroupMembershipKeysOfKafka09AreDeclared(): void
    {
        self::assertSame(11, ApiKeys::JOIN_GROUP);
        self::assertSame(12, ApiKeys::HEARTBEAT);
        self::assertSame(13, ApiKeys::LEAVE_GROUP);
        self::assertSame(14, ApiKeys::SYNC_GROUP);
        self::assertSame(15, ApiKeys::DESCRIBE_GROUPS);
        self::assertSame(16, ApiKeys::LIST_GROUPS);
    }

    /**
     * SaslHandshake (17), ApiVersions (18), CreateTopics (19) and DeleteTopics (20) arrived with Kafka 0.10
     */
    public function testTheKeysOfKafka010AreDeclared(): void
    {
        self::assertSame(17, ApiKeys::SASL_HANDSHAKE);
        self::assertSame(18, ApiKeys::API_VERSIONS);
        self::assertSame(19, ApiKeys::CREATE_TOPICS);
        self::assertSame(20, ApiKeys::DELETE_TOPICS);
    }

    /**
     * DeleteRecords (21) to AlterConfigs (33) arrived with Kafka 0.11
     */
    public function testTheKeysOfKafka011AreDeclared(): void
    {
        self::assertSame(21, ApiKeys::DELETE_RECORDS);
        self::assertSame(22, ApiKeys::INIT_PRODUCER_ID);
        self::assertSame(28, ApiKeys::TXN_OFFSET_COMMIT);
        self::assertSame(33, ApiKeys::ALTER_CONFIGS);
    }

    /**
     * AlterReplicaLogDirs (34), DescribeLogDirs (35), SaslAuthenticate (36) and CreatePartitions (37) arrived with
     * Kafka 1.0, the delegation token apis (38-41) and DeleteGroups (42) with Kafka 1.1
     */
    public function testTheKeysOfKafka1AreDeclared(): void
    {
        self::assertSame(34, ApiKeys::ALTER_REPLICA_LOG_DIRS);
        self::assertSame(35, ApiKeys::DESCRIBE_LOG_DIRS);
        self::assertSame(36, ApiKeys::SASL_AUTHENTICATE);
        self::assertSame(37, ApiKeys::CREATE_PARTITIONS);
        self::assertSame(38, ApiKeys::CREATE_DELEGATION_TOKEN);
        self::assertSame(41, ApiKeys::DESCRIBE_DELEGATION_TOKEN);
        self::assertSame(42, ApiKeys::DELETE_GROUPS);
    }

    /**
     * ElectLeaders (43) arrived with Kafka 2.2, IncrementalAlterConfigs (44) with 2.3, the partition reassignment
     * apis (45, 46) and OffsetDelete (47) with 2.4, the client quota apis (48, 49) with 2.6, the SCRAM credential
     * apis (50, 51), the raft apis (52-55), AlterIsr (56), UpdateFeatures (57) and Envelope (58) with 2.7, and
     * FetchSnapshot (59), DescribeCluster (60), DescribeProducers (61) and the broker registration apis (62-64)
     * with 2.8
     */
    public function testTheKeysOfKafka2AreDeclared(): void
    {
        self::assertSame(43, ApiKeys::ELECT_LEADERS);
        self::assertSame(44, ApiKeys::INCREMENTAL_ALTER_CONFIGS);
        self::assertSame(45, ApiKeys::ALTER_PARTITION_REASSIGNMENTS);
        self::assertSame(46, ApiKeys::LIST_PARTITION_REASSIGNMENTS);
        self::assertSame(47, ApiKeys::OFFSET_DELETE);
        self::assertSame(48, ApiKeys::DESCRIBE_CLIENT_QUOTAS);
        self::assertSame(49, ApiKeys::ALTER_CLIENT_QUOTAS);
        self::assertSame(50, ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS);
        self::assertSame(51, ApiKeys::ALTER_USER_SCRAM_CREDENTIALS);
        self::assertSame(52, ApiKeys::VOTE);
        self::assertSame(55, ApiKeys::DESCRIBE_QUORUM);
        self::assertSame(56, ApiKeys::ALTER_ISR);
        self::assertSame(57, ApiKeys::UPDATE_FEATURES);
        self::assertSame(58, ApiKeys::ENVELOPE);
        self::assertSame(59, ApiKeys::FETCH_SNAPSHOT);
        self::assertSame(60, ApiKeys::DESCRIBE_CLUSTER);
        self::assertSame(61, ApiKeys::DESCRIBE_PRODUCERS);
        self::assertSame(62, ApiKeys::BROKER_REGISTRATION);
        self::assertSame(64, ApiKeys::UNREGISTER_BROKER);
    }

    /**
     * The keys 65 to 87 of `ApiKeys.java` @ 3.9.2, read at the release tags: 65-67 with Kafka 3.0, 68 with 3.5, 69-74
     * with 3.7, 75 with 3.8 and 76-87 with 3.9
     */
    public function testTheKeysOfKafka3AreDeclared(): void
    {
        self::assertSame(65, ApiKeys::DESCRIBE_TRANSACTIONS);
        self::assertSame(66, ApiKeys::LIST_TRANSACTIONS);
        self::assertSame(67, ApiKeys::ALLOCATE_PRODUCER_IDS);
        self::assertSame(68, ApiKeys::CONSUMER_GROUP_HEARTBEAT);
        self::assertSame(69, ApiKeys::CONSUMER_GROUP_DESCRIBE);
        self::assertSame(70, ApiKeys::CONTROLLER_REGISTRATION);
        self::assertSame(71, ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS);
        self::assertSame(72, ApiKeys::PUSH_TELEMETRY);
        self::assertSame(73, ApiKeys::ASSIGN_REPLICAS_TO_DIRS);
        self::assertSame(74, ApiKeys::LIST_CLIENT_METRICS_RESOURCES);
        self::assertSame(75, ApiKeys::DESCRIBE_TOPIC_PARTITIONS);
        self::assertSame(76, ApiKeys::SHARE_GROUP_HEARTBEAT);
        self::assertSame(79, ApiKeys::SHARE_ACKNOWLEDGE);
        self::assertSame(80, ApiKeys::ADD_RAFT_VOTER);
        self::assertSame(82, ApiKeys::UPDATE_RAFT_VOTER);
        self::assertSame(83, ApiKeys::INITIALIZE_SHARE_GROUP_STATE);
        self::assertSame(87, ApiKeys::READ_SHARE_GROUP_STATE_SUMMARY);
    }

    /**
     * `ApiKeys.java` @ 3.9.2 ends at ReadShareGroupStateSummary (87); the keys Kafka 4.x adds must not appear on this
     * branch. ElectPreferredLeaders is the 2.2 name of ElectLeaders (43), AlterIsr the published name of key 56 (the
     * Java client renamed it AlterPartition in 3.2), and neither is a key of its own
     */
    public function testNoApiKeyOfALaterKafkaIsDeclared(): void
    {
        $keys = new ReflectionClass(ApiKeys::class)->getConstants();

        self::assertSame(range(0, 87), array_values($keys));
        self::assertNotContains('ELECT_PREFERRED_LEADERS', array_keys($keys));
        self::assertNotContains('ALTER_PARTITION', array_keys($keys));
    }
}
