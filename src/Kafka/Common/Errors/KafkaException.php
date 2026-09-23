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

use Exception;
use ReflectionObject;
use RuntimeException;

/**
 * Kafka uses numeric codes to indicate what problem occurred on the server.
 *
 * These can be translated by the client into exceptions or whatever the appropriate error handling mechanism in the
 * client language.
 *
 * The constant names are those of clients/src/main/java/org/apache/kafka/common/protocol/Errors.java @ 3.9.2,
 * which ends at 127: the 0.10 line added 32-35 (INVALID_TIMESTAMP, the two SASL codes and UNSUPPORTED_VERSION) with
 * 0.10.0, 36-42 (the CreateTopics codes, NOT_CONTROLLER and INVALID_REQUEST) with 0.10.1 and 43-44
 * (UNSUPPORTED_FOR_MESSAGE_FORMAT, POLICY_VIOLATION) with 0.10.2; Kafka 0.11 added 45-55, the codes of the
 * idempotent and transactional producer (KIP-98), of the ACL apis (SECURITY_DISABLED, OPERATION_NOT_ATTEMPTED) and
 * TRANSACTIONAL_ID_AUTHORIZATION_FAILED; of them only 46 is retriable, as in the Java client. Kafka 1.0 added 56-60
 * (KAFKA_STORAGE_ERROR and LOG_DIR_NOT_FOUND of the JBOD work, SASL_AUTHENTICATION_FAILED of SaslAuthenticate,
 * UNKNOWN_PRODUCER_ID, REASSIGNMENT_IN_PROGRESS) and Kafka 1.1 added 61-71 (the seven delegation token codes, the
 * two DeleteGroups codes and the two fetch session codes); of them 56, 70 and 71 are retriable, as in the Java client.
 * The class names are those of the Java client (`UnsupportedByAuthenticationException` for 64, `GroupNotEmptyException`
 * for 68), except 58, whose Java name `SaslAuthenticationException` is the client-side exception of this package.
 * Code 13 was StaleLeaderEpochCode in the 0.8 line and is NETWORK_EXCEPTION here; NO_ERROR is not part of the
 * mapping. The 2.x line added 72-104: 72 LISTENER_NOT_FOUND with Kafka 2.0; 73-76 (TOPIC_DELETION_DISABLED, the two
 * leader epoch codes of KIP-320, UNSUPPORTED_COMPRESSION_TYPE of the zstd codec) with 2.1; 77-81 (STALE_BROKER_EPOCH,
 * OFFSET_NOT_AVAILABLE, MEMBER_ID_REQUIRED, PREFERRED_LEADER_NOT_AVAILABLE, GROUP_MAX_SIZE_REACHED) with 2.2; 82
 * FENCED_INSTANCE_ID of the static membership with 2.3; 83-87 (the two ElectLeaders codes, NO_REASSIGNMENT_IN_PROGRESS,
 * GROUP_SUBSCRIBED_TO_TOPIC, INVALID_RECORD) with 2.4; 88 UNSTABLE_OFFSET_COMMIT with 2.5; 89-96
 * (THROTTLING_QUOTA_EXCEEDED, PRODUCER_FENCED, the three SCRAM credential codes, INCONSISTENT_VOTER_SET and the two
 * UpdateFeatures codes) with 2.7 and 97-104 (the forwarding, snapshot, topic id and broker registration codes of the
 * KRaft work) with 2.8. Of them 72, 74, 80, 83, 84, 100 and 103 extend InvalidMetadataException and 75, 78, 88 and 89
 * RetriableException in the Java client, as here. The 3.x line added 105-127, read off `Errors.java` at the release
 * tags: 105 TRANSACTIONAL_ID_NOT_FOUND with Kafka 3.0; 106 FETCH_SESSION_TOPIC_ID_ERROR with 3.1; 107-108 (the two
 * AlterPartition codes) with 3.3; 109-112 (OFFSET_MOVED_TO_TIERED_STORAGE of KIP-405 and the first three codes of the
 * KIP-848 consumer protocol) with 3.5; 113 STALE_MEMBER_EPOCH with 3.6; 114-119 (the three endpoint codes of KIP-919,
 * the two client-metrics codes of KIP-714 and INVALID_REGISTRATION) with 3.7; 120 TRANSACTION_ABORTABLE of KIP-890
 * with 3.8 and 121-127 (the four share-group codes of KIP-932 and the three voter codes of KIP-853) with 3.9. Of them
 * only 106, 122 and 123 extend RetriableException in the Java client, as here. Codes above 127 are answered with
 * {@see \Protocol\Kafka\Common\Errors\UnknownErrorException} by {@see self::fromCode()}.
 */
abstract class KafkaException extends RuntimeException
{
    public const UNKNOWN = -1;

    public const NO_ERROR = 0;

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
    public const INVALID_TIMESTAMP                  = 32;
    public const UNSUPPORTED_SASL_MECHANISM         = 33;
    public const ILLEGAL_SASL_STATE                 = 34;
    public const UNSUPPORTED_VERSION                = 35;
    public const TOPIC_ALREADY_EXISTS               = 36;
    public const INVALID_PARTITIONS                 = 37;
    public const INVALID_REPLICATION_FACTOR         = 38;
    public const INVALID_REPLICA_ASSIGNMENT         = 39;
    public const INVALID_CONFIG                     = 40;
    public const NOT_CONTROLLER                     = 41;
    public const INVALID_REQUEST                    = 42;
    public const UNSUPPORTED_FOR_MESSAGE_FORMAT     = 43;
    public const POLICY_VIOLATION                   = 44;
    public const OUT_OF_ORDER_SEQUENCE_NUMBER          = 45;
    public const DUPLICATE_SEQUENCE_NUMBER             = 46;
    public const INVALID_PRODUCER_EPOCH                = 47;
    public const INVALID_TXN_STATE                     = 48;
    public const INVALID_PRODUCER_ID_MAPPING           = 49;
    public const INVALID_TRANSACTION_TIMEOUT           = 50;
    public const CONCURRENT_TRANSACTIONS               = 51;
    public const TRANSACTION_COORDINATOR_FENCED        = 52;
    public const TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53;
    public const SECURITY_DISABLED                     = 54;
    public const OPERATION_NOT_ATTEMPTED               = 55;
    public const KAFKA_STORAGE_ERROR                     = 56;
    public const LOG_DIR_NOT_FOUND                       = 57;
    public const SASL_AUTHENTICATION_FAILED              = 58;
    public const UNKNOWN_PRODUCER_ID                     = 59;
    public const REASSIGNMENT_IN_PROGRESS                = 60;
    public const DELEGATION_TOKEN_AUTH_DISABLED          = 61;
    public const DELEGATION_TOKEN_NOT_FOUND              = 62;
    public const DELEGATION_TOKEN_OWNER_MISMATCH         = 63;
    public const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED    = 64;
    public const DELEGATION_TOKEN_AUTHORIZATION_FAILED   = 65;
    public const DELEGATION_TOKEN_EXPIRED                = 66;
    public const INVALID_PRINCIPAL_TYPE                  = 67;
    public const NON_EMPTY_GROUP                         = 68;
    public const GROUP_ID_NOT_FOUND                      = 69;
    public const FETCH_SESSION_ID_NOT_FOUND              = 70;
    public const INVALID_FETCH_SESSION_EPOCH             = 71;
    public const LISTENER_NOT_FOUND                    = 72;
    public const TOPIC_DELETION_DISABLED               = 73;
    public const FENCED_LEADER_EPOCH                   = 74;
    public const UNKNOWN_LEADER_EPOCH                  = 75;
    public const UNSUPPORTED_COMPRESSION_TYPE          = 76;
    public const STALE_BROKER_EPOCH                    = 77;
    public const OFFSET_NOT_AVAILABLE                  = 78;
    public const MEMBER_ID_REQUIRED                    = 79;
    public const PREFERRED_LEADER_NOT_AVAILABLE        = 80;
    public const GROUP_MAX_SIZE_REACHED                = 81;
    public const FENCED_INSTANCE_ID                    = 82;
    public const ELIGIBLE_LEADERS_NOT_AVAILABLE        = 83;
    public const ELECTION_NOT_NEEDED                   = 84;
    public const NO_REASSIGNMENT_IN_PROGRESS           = 85;
    public const GROUP_SUBSCRIBED_TO_TOPIC             = 86;
    public const INVALID_RECORD                        = 87;
    public const UNSTABLE_OFFSET_COMMIT                = 88;
    public const THROTTLING_QUOTA_EXCEEDED             = 89;
    public const PRODUCER_FENCED                       = 90;
    public const RESOURCE_NOT_FOUND                    = 91;
    public const DUPLICATE_RESOURCE                    = 92;
    public const UNACCEPTABLE_CREDENTIAL               = 93;
    public const INCONSISTENT_VOTER_SET                = 94;
    public const INVALID_UPDATE_VERSION                = 95;
    public const FEATURE_UPDATE_FAILED                 = 96;
    public const PRINCIPAL_DESERIALIZATION_FAILURE     = 97;
    public const SNAPSHOT_NOT_FOUND                    = 98;
    public const POSITION_OUT_OF_RANGE                 = 99;
    public const UNKNOWN_TOPIC_ID                      = 100;
    public const DUPLICATE_BROKER_REGISTRATION         = 101;
    public const BROKER_ID_NOT_REGISTERED              = 102;
    public const INCONSISTENT_TOPIC_ID                 = 103;
    public const INCONSISTENT_CLUSTER_ID               = 104;
    public const TRANSACTIONAL_ID_NOT_FOUND             = 105;
    public const FETCH_SESSION_TOPIC_ID_ERROR           = 106;
    public const INELIGIBLE_REPLICA                     = 107;
    public const NEW_LEADER_ELECTED                     = 108;
    public const OFFSET_MOVED_TO_TIERED_STORAGE         = 109;
    public const FENCED_MEMBER_EPOCH                    = 110;
    public const UNRELEASED_INSTANCE_ID                 = 111;
    public const UNSUPPORTED_ASSIGNOR                   = 112;
    public const STALE_MEMBER_EPOCH                     = 113;
    public const MISMATCHED_ENDPOINT_TYPE               = 114;
    public const UNSUPPORTED_ENDPOINT_TYPE              = 115;
    public const UNKNOWN_CONTROLLER_ID                  = 116;
    public const UNKNOWN_SUBSCRIPTION_ID                = 117;
    public const TELEMETRY_TOO_LARGE                    = 118;
    public const INVALID_REGISTRATION                   = 119;
    public const TRANSACTION_ABORTABLE                  = 120;
    public const INVALID_RECORD_STATE                   = 121;
    public const SHARE_SESSION_NOT_FOUND                = 122;
    public const INVALID_SHARE_SESSION_EPOCH            = 123;
    public const FENCED_STATE_EPOCH                     = 124;
    public const INVALID_VOTER_KEY                      = 125;
    public const DUPLICATE_VOTER                        = 126;
    public const VOTER_NOT_FOUND                        = 127;

    /**
     * Mapping from the codes to class names
     *
     * @var array<int, class-string<KafkaException>>
     */
    private static array $codeToClassMap = [
        self::UNKNOWN                          => UnknownErrorException::class,
        self::OFFSET_OUT_OF_RANGE              => OffsetOutOfRangeException::class,
        self::CORRUPT_MESSAGE                  => CorruptMessageException::class,
        self::UNKNOWN_TOPIC_OR_PARTITION       => UnknownTopicOrPartitionException::class,
        self::INVALID_FETCH_SIZE               => InvalidFetchSizeException::class,
        self::LEADER_NOT_AVAILABLE             => LeaderNotAvailableException::class,
        self::NOT_LEADER_FOR_PARTITION         => NotLeaderForPartitionException::class,
        self::REQUEST_TIMED_OUT                => RequestTimedOutException::class,
        self::BROKER_NOT_AVAILABLE             => BrokerNotAvailableException::class,
        self::REPLICA_NOT_AVAILABLE            => ReplicaNotAvailableException::class,
        self::MESSAGE_TOO_LARGE                => MessageTooLargeException::class,
        self::STALE_CONTROLLER_EPOCH           => StaleControllerEpochException::class,
        self::OFFSET_METADATA_TOO_LARGE        => OffsetMetadataTooLargeException::class,
        self::NETWORK_EXCEPTION                => NetworkException::class,
        self::GROUP_LOAD_IN_PROGRESS           => GroupLoadInProgressException::class,
        self::GROUP_COORDINATOR_NOT_AVAILABLE  => GroupCoordinatorNotAvailableException::class,
        self::NOT_COORDINATOR_FOR_GROUP        => NotCoordinatorForGroupException::class,
        self::INVALID_TOPIC_EXCEPTION          => InvalidTopicException::class,
        self::RECORD_LIST_TOO_LARGE            => RecordListTooLargeException::class,
        self::NOT_ENOUGH_REPLICAS              => NotEnoughReplicasException::class,
        self::NOT_ENOUGH_REPLICAS_AFTER_APPEND => NotEnoughReplicasAfterAppendException::class,
        self::INVALID_REQUIRED_ACKS            => InvalidRequiredAcksException::class,
        self::ILLEGAL_GENERATION               => IllegalGenerationException::class,
        self::INCONSISTENT_GROUP_PROTOCOL      => InconsistentGroupProtocolException::class,
        self::INVALID_GROUP_ID                 => InvalidGroupIdException::class,
        self::UNKNOWN_MEMBER_ID                => UnknownMemberIdException::class,
        self::INVALID_SESSION_TIMEOUT          => InvalidSessionTimeoutException::class,
        self::REBALANCE_IN_PROGRESS            => RebalanceInProgressException::class,
        self::INVALID_COMMIT_OFFSET_SIZE       => InvalidCommitOffsetSizeException::class,
        self::TOPIC_AUTHORIZATION_FAILED       => TopicAuthorizationFailedException::class,
        self::GROUP_AUTHORIZATION_FAILED       => GroupAuthorizationFailedException::class,
        self::CLUSTER_AUTHORIZATION_FAILED     => ClusterAuthorizationFailedException::class,
        self::INVALID_TIMESTAMP                  => InvalidTimestampException::class,
        self::UNSUPPORTED_SASL_MECHANISM         => UnsupportedSaslMechanismException::class,
        self::ILLEGAL_SASL_STATE                 => IllegalSaslStateException::class,
        self::UNSUPPORTED_VERSION                => UnsupportedVersionException::class,
        self::TOPIC_ALREADY_EXISTS               => TopicExistsException::class,
        self::INVALID_PARTITIONS                 => InvalidPartitionsException::class,
        self::INVALID_REPLICATION_FACTOR         => InvalidReplicationFactorException::class,
        self::INVALID_REPLICA_ASSIGNMENT         => InvalidReplicaAssignmentException::class,
        self::INVALID_CONFIG                     => InvalidConfigException::class,
        self::NOT_CONTROLLER                     => NotControllerException::class,
        self::INVALID_REQUEST                    => InvalidRequestException::class,
        self::UNSUPPORTED_FOR_MESSAGE_FORMAT     => UnsupportedForMessageFormatException::class,
        self::POLICY_VIOLATION                   => PolicyViolationException::class,
        self::OUT_OF_ORDER_SEQUENCE_NUMBER          => OutOfOrderSequenceException::class,
        self::DUPLICATE_SEQUENCE_NUMBER             => DuplicateSequenceNumberException::class,
        self::INVALID_PRODUCER_EPOCH                => ProducerFencedException::class,
        self::INVALID_TXN_STATE                     => InvalidTxnStateException::class,
        self::INVALID_PRODUCER_ID_MAPPING           => InvalidPidMappingException::class,
        self::INVALID_TRANSACTION_TIMEOUT           => InvalidTxnTimeoutException::class,
        self::CONCURRENT_TRANSACTIONS               => ConcurrentTransactionsException::class,
        self::TRANSACTION_COORDINATOR_FENCED        => TransactionCoordinatorFencedException::class,
        self::TRANSACTIONAL_ID_AUTHORIZATION_FAILED => TransactionalIdAuthorizationException::class,
        self::SECURITY_DISABLED                     => SecurityDisabledException::class,
        self::OPERATION_NOT_ATTEMPTED               => OperationNotAttemptedException::class,
        self::KAFKA_STORAGE_ERROR                     => KafkaStorageException::class,
        self::LOG_DIR_NOT_FOUND                       => LogDirNotFoundException::class,
        self::SASL_AUTHENTICATION_FAILED              => SaslAuthenticationFailedException::class,
        self::UNKNOWN_PRODUCER_ID                     => UnknownProducerIdException::class,
        self::REASSIGNMENT_IN_PROGRESS                => ReassignmentInProgressException::class,
        self::DELEGATION_TOKEN_AUTH_DISABLED          => DelegationTokenDisabledException::class,
        self::DELEGATION_TOKEN_NOT_FOUND              => DelegationTokenNotFoundException::class,
        self::DELEGATION_TOKEN_OWNER_MISMATCH         => DelegationTokenOwnerMismatchException::class,
        self::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED    => UnsupportedByAuthenticationException::class,
        self::DELEGATION_TOKEN_AUTHORIZATION_FAILED   => DelegationTokenAuthorizationException::class,
        self::DELEGATION_TOKEN_EXPIRED                => DelegationTokenExpiredException::class,
        self::INVALID_PRINCIPAL_TYPE                  => InvalidPrincipalTypeException::class,
        self::NON_EMPTY_GROUP                         => GroupNotEmptyException::class,
        self::GROUP_ID_NOT_FOUND                      => GroupIdNotFoundException::class,
        self::FETCH_SESSION_ID_NOT_FOUND              => FetchSessionIdNotFoundException::class,
        self::INVALID_FETCH_SESSION_EPOCH             => InvalidFetchSessionEpochException::class,
        self::LISTENER_NOT_FOUND                    => ListenerNotFoundException::class,
        self::TOPIC_DELETION_DISABLED               => TopicDeletionDisabledException::class,
        self::FENCED_LEADER_EPOCH                   => FencedLeaderEpochException::class,
        self::UNKNOWN_LEADER_EPOCH                  => UnknownLeaderEpochException::class,
        self::UNSUPPORTED_COMPRESSION_TYPE          => UnsupportedCompressionTypeException::class,
        self::STALE_BROKER_EPOCH                    => StaleBrokerEpochException::class,
        self::OFFSET_NOT_AVAILABLE                  => OffsetNotAvailableException::class,
        self::MEMBER_ID_REQUIRED                    => MemberIdRequiredException::class,
        self::PREFERRED_LEADER_NOT_AVAILABLE        => PreferredLeaderNotAvailableException::class,
        self::GROUP_MAX_SIZE_REACHED                => GroupMaxSizeReachedException::class,
        self::FENCED_INSTANCE_ID                    => FencedInstanceIdException::class,
        self::ELIGIBLE_LEADERS_NOT_AVAILABLE        => EligibleLeadersNotAvailableException::class,
        self::ELECTION_NOT_NEEDED                   => ElectionNotNeededException::class,
        self::NO_REASSIGNMENT_IN_PROGRESS           => NoReassignmentInProgressException::class,
        self::GROUP_SUBSCRIBED_TO_TOPIC             => GroupSubscribedToTopicException::class,
        self::INVALID_RECORD                        => InvalidRecordException::class,
        self::UNSTABLE_OFFSET_COMMIT                => UnstableOffsetCommitException::class,
        self::THROTTLING_QUOTA_EXCEEDED             => ThrottlingQuotaExceededException::class,
        self::PRODUCER_FENCED                       => TransactionalProducerFencedException::class,
        self::RESOURCE_NOT_FOUND                    => ResourceNotFoundException::class,
        self::DUPLICATE_RESOURCE                    => DuplicateResourceException::class,
        self::UNACCEPTABLE_CREDENTIAL               => UnacceptableCredentialException::class,
        self::INCONSISTENT_VOTER_SET                => InconsistentVoterSetException::class,
        self::INVALID_UPDATE_VERSION                => InvalidUpdateVersionException::class,
        self::FEATURE_UPDATE_FAILED                 => FeatureUpdateFailedException::class,
        self::PRINCIPAL_DESERIALIZATION_FAILURE     => PrincipalDeserializationException::class,
        self::SNAPSHOT_NOT_FOUND                    => SnapshotNotFoundException::class,
        self::POSITION_OUT_OF_RANGE                 => PositionOutOfRangeException::class,
        self::UNKNOWN_TOPIC_ID                      => UnknownTopicIdException::class,
        self::DUPLICATE_BROKER_REGISTRATION         => DuplicateBrokerRegistrationException::class,
        self::BROKER_ID_NOT_REGISTERED              => BrokerIdNotRegisteredException::class,
        self::INCONSISTENT_TOPIC_ID                 => InconsistentTopicIdException::class,
        self::INCONSISTENT_CLUSTER_ID               => InconsistentClusterIdException::class,
        self::TRANSACTIONAL_ID_NOT_FOUND             => TransactionalIdNotFoundException::class,
        self::FETCH_SESSION_TOPIC_ID_ERROR           => FetchSessionTopicIdException::class,
        self::INELIGIBLE_REPLICA                     => IneligibleReplicaException::class,
        self::NEW_LEADER_ELECTED                     => NewLeaderElectedException::class,
        self::OFFSET_MOVED_TO_TIERED_STORAGE         => OffsetMovedToTieredStorageException::class,
        self::FENCED_MEMBER_EPOCH                    => FencedMemberEpochException::class,
        self::UNRELEASED_INSTANCE_ID                 => UnreleasedInstanceIdException::class,
        self::UNSUPPORTED_ASSIGNOR                   => UnsupportedAssignorException::class,
        self::STALE_MEMBER_EPOCH                     => StaleMemberEpochException::class,
        self::MISMATCHED_ENDPOINT_TYPE               => MismatchedEndpointTypeException::class,
        self::UNSUPPORTED_ENDPOINT_TYPE              => UnsupportedEndpointTypeException::class,
        self::UNKNOWN_CONTROLLER_ID                  => UnknownControllerIdException::class,
        self::UNKNOWN_SUBSCRIPTION_ID                => UnknownSubscriptionIdException::class,
        self::TELEMETRY_TOO_LARGE                    => TelemetryTooLargeException::class,
        self::INVALID_REGISTRATION                   => InvalidRegistrationException::class,
        self::TRANSACTION_ABORTABLE                  => TransactionAbortableException::class,
        self::INVALID_RECORD_STATE                   => InvalidRecordStateException::class,
        self::SHARE_SESSION_NOT_FOUND                => ShareSessionNotFoundException::class,
        self::INVALID_SHARE_SESSION_EPOCH            => InvalidShareSessionEpochException::class,
        self::FENCED_STATE_EPOCH                     => FencedStateEpochException::class,
        self::INVALID_VOTER_KEY                      => InvalidVoterKeyException::class,
        self::DUPLICATE_VOTER                        => DuplicateVoterException::class,
        self::VOTER_NOT_FOUND                        => VoterNotFoundException::class,
    ];

    /**
     * Additional context for the exception
     *
     * @var array
     */
    private array $context = [];

    /**
     * Creates an instance of exception by error code
     *
     * @param integer        $errorCode Error code from the Kafka
     * @param array          $context   Additional context
     * @param Exception|null $previous
     *
     * @return KafkaException
     */
    final public static function fromCode(int $errorCode, array $context = [], ?Exception $previous = null): KafkaException
    {
        if (!isset(self::$codeToClassMap[$errorCode])) {
            return new UnknownErrorException(['errorCode' => $errorCode] + $context, $previous);
        }
        $exceptionClass = self::$codeToClassMap[$errorCode];

        return new $exceptionClass($context, $previous);
    }

    /**
     * @inheritDoc
     */
    public function __construct(array $context = [], int $code = self::UNKNOWN, ?Exception $previous = null)
    {
        $this->context = $context;

        $docBlock = new ReflectionObject($this)->getDocComment() ?: '';
        $docBlock = (string) preg_replace('/^\s*\/?\*+\/?/m', '', $docBlock);
        $docBlock = trim((string) preg_replace('/\s{2,}/', ' ', $docBlock));

        $message = $docBlock . PHP_EOL . 'Context: ' . json_encode($context);

        parent::__construct($message, $code, $previous);
    }

    /**
     * Returns the context for this exception
     */
    public function getContext(): array
    {
        return $this->context;
    }
}
