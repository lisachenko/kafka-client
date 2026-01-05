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

namespace Protocol\Kafka\Common\Errors;

/**
 * Kafka uses numeric codes to indicate what problem occurred on the server.
 *
 * These can be translated by the client into exceptions or whatever the appropriate error handling mechanism in the
 * client language.
 */
interface KafkaException
{
    public const UNKNOWN = -1;

    public const OFFSET_OUT_OF_RANGE              = 1;
    public const CORRUPT_MESSAGE                  = 2;
    public const UNKNOWN_TOPIC_OR_PARTITION       = 3;
    public const INVALID_FETCH_SIZE               = 4;
    public const LEADER_NOT_AVAILABLE             = 5;
    public const NOT_LEADER_FOR_PARTITION         = 6;
    public const REQUEST_TIMED_OUT                = 7;
    public const BROKER_NOT_AVAILABLE             = 8;
    public const REPLICA_NOT_AVAILABLE            = 9;
    public const MESSAGE_TOO_LARGE                = 10;
    public const STALE_CONTROLLER_EPOCH           = 11;
    public const OFFSET_METADATA_TOO_LARGE        = 12;
    public const NETWORK_EXCEPTION                = 13;
    public const GROUP_LOAD_IN_PROGRESS           = 14;
    public const GROUP_COORDINATOR_NOT_AVAILABLE  = 15;
    public const NOT_COORDINATOR_FOR_GROUP        = 16;
    public const INVALID_TOPIC_EXCEPTION          = 17;
    public const RECORD_LIST_TOO_LARGE            = 18;
    public const NOT_ENOUGH_REPLICAS              = 19;
    public const NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
    public const INVALID_REQUIRED_ACKS            = 21;
    public const ILLEGAL_GENERATION               = 22;
    public const INCONSISTENT_GROUP_PROTOCOL      = 23;
    public const INVALID_GROUP_ID                 = 24;
    public const UNKNOWN_MEMBER_ID                = 25;
    public const INVALID_SESSION_TIMEOUT          = 26;
    public const REBALANCE_IN_PROGRESS            = 27;
    public const INVALID_COMMIT_OFFSET_SIZE       = 28;
    public const TOPIC_AUTHORIZATION_FAILED       = 29;
    public const GROUP_AUTHORIZATION_FAILED       = 30;
    public const CLUSTER_AUTHORIZATION_FAILED     = 31;
    public const INVALID_TIMESTAMP                = 32;
    public const UNSUPPORTED_SASL_MECHANISM       = 33;
    public const ILLEGAL_SASL_STATE               = 34;
    public const UNSUPPORTED_VERSION              = 35;
}
