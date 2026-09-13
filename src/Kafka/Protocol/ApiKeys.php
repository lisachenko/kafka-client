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
 * Numeric codes that the ApiKey in the request can take, as of Kafka 2.8.2.
 *
 * The list mirrors org.apache.kafka.common.protocol.ApiKeys @ 2.8.2: SaslHandshake (17) and ApiVersions (18)
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
 * A broker of 0.10 or later answers an ApiVersions request with the keys and versions it serves; a request with a
 * key or version it cannot parse closes the connection. A ZooKeeper-backed 2.8.2 broker serves the keys 0 to 51, 56,
 * 57, 60 and 61 (the `zkBroker` listener of the message specifications); the raft, envelope, snapshot and broker
 * registration apis (52 to 55, 58, 59, 62 to 64) are the KRaft controller's and never appear in its ApiVersions
 * answer - their constants exist here so that a frame of any 2.8.2 api can be named.
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
}
