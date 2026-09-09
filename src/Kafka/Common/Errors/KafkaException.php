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
 * The constant names are those of the later protocol lines so that the cascade merge stays small; the codes and the
 * set of codes are those of clients/src/main/java/org/apache/kafka/common/protocol/Errors.java @ 0.9.0.1, which ends
 * at 31. Code 13 was StaleLeaderEpochCode in the 0.8 line and is NETWORK_EXCEPTION here; NO_ERROR is not part of the
 * mapping. Codes 32 and above (INVALID_TIMESTAMP, the SASL codes, UNSUPPORTED_VERSION) arrived with Kafka 0.10.
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
