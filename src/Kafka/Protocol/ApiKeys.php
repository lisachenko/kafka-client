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
 * Numeric codes that the ApiKey in the request can take, as of Kafka 0.11.0.3.
 *
 * The list mirrors org.apache.kafka.common.protocol.ApiKeys @ 0.11.0.3: SaslHandshake (17) and ApiVersions (18)
 * arrived with Kafka 0.10.0, CreateTopics (19) and DeleteTopics (20) with 0.10.1, and the keys 21 to 33 (DeleteRecords,
 * the idempotent and transactional producer apis, the ACL and config apis) with 0.11.0. The constants above 20 were
 * kept by the cascade merge of `0.10.x` into this line; their requests are implemented by the tickets of the 0.11
 * line (see docs/handoff/main.md). A broker of 0.10 or later answers an ApiVersions request with the keys and
 * versions it serves; a request with a key or version it cannot parse closes the connection.
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
}
