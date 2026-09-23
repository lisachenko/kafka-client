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

/**
 * @author Alexander.Lisachenko
 */

namespace Protocol\Kafka\Protocol;

/**
 * Numeric codes that the ApiKey in the request can take, as of Kafka 4.3.1.
 *
 * The list mirrors org.apache.kafka.common.protocol.ApiKeys @ 4.3.1: SaslHandshake (17) and ApiVersions (18)
 * arrived with Kafka 0.10.0, CreateTopics (19) and DeleteTopics (20) with 0.10.1, the keys 21 to 33 (DeleteRecords,
 * the idempotent and transactional producer apis, the ACL and config apis) with 0.11.0, the keys 34 to 37
 * (AlterReplicaLogDirs and DescribeLogDirs of KIP-113, SaslAuthenticate of KIP-152, CreatePartitions of KIP-195)
 * with 1.0.0 and the keys 38 to 42 (the delegation token apis of KIP-48, DeleteGroups of KIP-229) with 1.1.0.
 * Kafka 2.0 and 2.1 added no key; 2.2 added ElectLeaders (43, KIP-183, called ElectPreferredLeaders until 2.4), 2.3
 * IncrementalAlterConfigs (44, KIP-339), 2.4 the two partition reassignment apis (45 and 46, KIP-455) and OffsetDelete
 * (47, KIP-496), 2.6 the two client quota apis (48 and 49, KIP-546), 2.7 the two SCRAM credential apis (50 and 51,
 * KIP-554), the four raft apis of the KRaft quorum (52 to 55, KIP-595), AlterIsr (56, KIP-497), UpdateFeatures (57,
 * KIP-584) and Envelope (58, KIP-590), and 2.8 FetchSnapshot (59, KIP-630), DescribeCluster (60, KIP-700),
 * DescribeProducers (61, KIP-664) and the three broker registration apis of the KRaft mode (62 to 64, KIP-631).
 * The 3.x line added the keys 65 to 87 (`ApiKeys.java` @ 3.9.2, read at the release tags): 3.0 DescribeTransactions
 * (65), ListTransactions (66) and AllocateProducerIds (67, broker-to-controller); 3.5 ConsumerGroupHeartbeat (68) of
 * the new consumer protocol of KIP-848; 3.7 ConsumerGroupDescribe (69, KIP-848), ControllerRegistration (70, KIP-919),
 * the client-metrics apis GetTelemetrySubscriptions (71), PushTelemetry (72) and ListClientMetricsResources (74) of
 * KIP-714 and AssignReplicasToDirs (73); 3.8 DescribeTopicPartitions (75); and 3.9 the share-group apis
 * ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch and ShareAcknowledge (76 to 79, KIP-932, early access) with
 * their state apis 83 to 87 (internal), and the raft-voter apis AddRaftVoter, RemoveRaftVoter and UpdateRaftVoter
 * (80 to 82, the dynamic quorums of KIP-853). Key 56 was renamed from AlterIsr to AlterPartition in Kafka 3.2
 * (KIP-704); the constant keeps the published name.
 * The 4.x line adds the keys 88 to 92 (`ApiKeys.java` @ 4.3.1, read at the release tags), all of them Kafka 4.1: the
 * streams-group apis StreamsGroupHeartbeat (88) and StreamsGroupDescribe (89) of KIP-1071 (unstable in 4.1, stable
 * from 4.2) and the share-group offset apis DescribeShareGroupOffsets (90), AlterShareGroupOffsets (91) and
 * DeleteShareGroupOffsets (92) of KIP-932. Kafka 4.0 removed the ZooKeeper-only apis 4 to 7 from every listener
 * (their message specifications declare `"validVersions": "none"`, the constants stay) and 4.1 renamed key 74 from
 * ListClientMetricsResources to ListConfigResources (KIP-1142); the constant keeps the published name.
 * A broker of 0.10 or later answers an ApiVersions request with the keys and versions it serves; a request with a
 * key or version it cannot parse closes the connection. The answer is the set of the **listener** the request arrived
 * on: a ZooKeeper-backed broker serves the `zkBroker` set of the message specifications, a KRaft node the `broker`
 * set on its client listeners (and the `controller` set on its controller listener, which no client reaches). The
 * container of this line is a KRaft node of 4.3.1; what it lists is measured in `ApiVersionProbeTest` and written in
 * the "API keys" section of `docs/protocol/4.3.md`. The constants of the apis it does not list exist here so that a
 * frame of any 4.3.1 api can be named.
 */
class ApiKeys
{
    /**
     * The following are the numeric codes that the ApiKey in the request can take for each of the below request types.
     *
     * The names are those of the later protocol lines; key 10 is called ConsumerMetadata in Kafka 0.8.2 and was
     * renamed to GroupCoordinator in 0.9 (kafka/api/RequestKeys.scala @ 0.9.0.1) without a wire format change.
     */
    public const PRODUCE                 = 0;
    public const FETCH                   = 1;
    public const OFFSETS                 = 2;
    public const METADATA                = 3;
    public const LEADER_AND_ISR          = 4;
    public const STOP_REPLICA            = 5;
    public const UPDATE_METADATA         = 6;
    public const CONTROLLED_SHUTDOWN     = 7;
    public const OFFSET_COMMIT           = 8;
    public const OFFSET_FETCH            = 9;
    public const GROUP_COORDINATOR       = 10;
    public const JOIN_GROUP              = 11;
    public const HEARTBEAT               = 12;
    public const LEAVE_GROUP             = 13;
    public const SYNC_GROUP              = 14;
    public const DESCRIBE_GROUPS         = 15;
    public const LIST_GROUPS             = 16;
    public const SASL_HANDSHAKE          = 17;
    public const API_VERSIONS            = 18;
    public const CREATE_TOPICS           = 19;
    public const DELETE_TOPICS           = 20;
    public const DELETE_RECORDS          = 21;
    public const INIT_PRODUCER_ID        = 22;
    public const OFFSET_FOR_LEADER_EPOCH = 23;
    public const ADD_PARTITIONS_TO_TXN   = 24;
    public const ADD_OFFSETS_TO_TXN      = 25;
    public const END_TXN                 = 26;
    public const WRITE_TXN_MARKERS       = 27;
    public const TXN_OFFSET_COMMIT       = 28;
    public const DESCRIBE_ACLS           = 29;
    public const CREATE_ACLS             = 30;
    public const DELETE_ACLS             = 31;
    public const DESCRIBE_CONFIGS        = 32;
    public const ALTER_CONFIGS           = 33;
    public const ALTER_REPLICA_LOG_DIRS  = 34;
    public const DESCRIBE_LOG_DIRS       = 35;
    public const SASL_AUTHENTICATE       = 36;
    public const CREATE_PARTITIONS       = 37;
    public const CREATE_DELEGATION_TOKEN   = 38;
    public const RENEW_DELEGATION_TOKEN    = 39;
    public const EXPIRE_DELEGATION_TOKEN   = 40;
    public const DESCRIBE_DELEGATION_TOKEN = 41;
    public const DELETE_GROUPS             = 42;
    public const ELECT_LEADERS                    = 43;
    public const INCREMENTAL_ALTER_CONFIGS        = 44;
    public const ALTER_PARTITION_REASSIGNMENTS    = 45;
    public const LIST_PARTITION_REASSIGNMENTS     = 46;
    public const OFFSET_DELETE                    = 47;
    public const DESCRIBE_CLIENT_QUOTAS           = 48;
    public const ALTER_CLIENT_QUOTAS              = 49;
    public const DESCRIBE_USER_SCRAM_CREDENTIALS  = 50;
    public const ALTER_USER_SCRAM_CREDENTIALS     = 51;
    public const VOTE                             = 52;
    public const BEGIN_QUORUM_EPOCH               = 53;
    public const END_QUORUM_EPOCH                 = 54;
    public const DESCRIBE_QUORUM                  = 55;
    public const ALTER_ISR                        = 56;
    public const UPDATE_FEATURES                  = 57;
    public const ENVELOPE                         = 58;
    public const FETCH_SNAPSHOT                   = 59;
    public const DESCRIBE_CLUSTER                 = 60;
    public const DESCRIBE_PRODUCERS               = 61;
    public const BROKER_REGISTRATION              = 62;
    public const BROKER_HEARTBEAT                 = 63;
    public const UNREGISTER_BROKER                = 64;
    public const DESCRIBE_TRANSACTIONS            = 65;
    public const LIST_TRANSACTIONS                = 66;
    public const ALLOCATE_PRODUCER_IDS            = 67;
    public const CONSUMER_GROUP_HEARTBEAT         = 68;
    public const CONSUMER_GROUP_DESCRIBE          = 69;
    public const CONTROLLER_REGISTRATION          = 70;
    public const GET_TELEMETRY_SUBSCRIPTIONS      = 71;
    public const PUSH_TELEMETRY                   = 72;
    public const ASSIGN_REPLICAS_TO_DIRS          = 73;
    public const LIST_CLIENT_METRICS_RESOURCES    = 74;
    public const DESCRIBE_TOPIC_PARTITIONS        = 75;
    public const SHARE_GROUP_HEARTBEAT            = 76;
    public const SHARE_GROUP_DESCRIBE             = 77;
    public const SHARE_FETCH                      = 78;
    public const SHARE_ACKNOWLEDGE                = 79;
    public const ADD_RAFT_VOTER                   = 80;
    public const REMOVE_RAFT_VOTER                = 81;
    public const UPDATE_RAFT_VOTER                = 82;
    public const INITIALIZE_SHARE_GROUP_STATE     = 83;
    public const READ_SHARE_GROUP_STATE           = 84;
    public const WRITE_SHARE_GROUP_STATE          = 85;
    public const DELETE_SHARE_GROUP_STATE         = 86;
    public const READ_SHARE_GROUP_STATE_SUMMARY   = 87;
    public const STREAMS_GROUP_HEARTBEAT          = 88;
    public const STREAMS_GROUP_DESCRIBE           = 89;
    public const DESCRIBE_SHARE_GROUP_OFFSETS     = 90;
    public const ALTER_SHARE_GROUP_OFFSETS        = 91;
    public const DELETE_SHARE_GROUP_OFFSETS       = 92;
}
