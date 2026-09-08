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
 * The list of codes is the complete one of Kafka 0.8.2.2, see kafka/common/ErrorMapping.scala at that tag. Codes
 * above 20 were introduced by 0.9 and later and therefore never appear on this protocol line.
 */
abstract class KafkaException extends RuntimeException
{
    public const NO_ERROR = 0;
    public const UNKNOWN  = -1;

    public const OFFSET_OUT_OF_RANGE                = 1;
    public const CORRUPT_MESSAGE                    = 2;
    public const UNKNOWN_TOPIC_OR_PARTITION         = 3;
    public const INVALID_FETCH_SIZE                 = 4;
    public const LEADER_NOT_AVAILABLE               = 5;
    public const NOT_LEADER_FOR_PARTITION           = 6;
    public const REQUEST_TIMED_OUT                  = 7;
    public const BROKER_NOT_AVAILABLE               = 8;
    public const REPLICA_NOT_AVAILABLE              = 9;
    public const MESSAGE_SIZE_TOO_LARGE             = 10;
    public const STALE_CONTROLLER_EPOCH             = 11;
    public const OFFSET_METADATA_TOO_LARGE          = 12;
    public const STALE_LEADER_EPOCH                 = 13;
    public const OFFSETS_LOAD_IN_PROGRESS           = 14;
    public const CONSUMER_COORDINATOR_NOT_AVAILABLE = 15;
    public const NOT_COORDINATOR_FOR_CONSUMER       = 16;
    public const INVALID_TOPIC                      = 17;
    public const MESSAGE_SET_SIZE_TOO_LARGE         = 18;
    public const NOT_ENOUGH_REPLICAS                = 19;
    public const NOT_ENOUGH_REPLICAS_AFTER_APPEND   = 20;

    /**
     * Mapping from the codes to class names
     *
     * @var array<int, class-string<KafkaException>>
     */
    private static array $codeToClassMap = [
        self::UNKNOWN                            => UnknownErrorException::class,
        self::OFFSET_OUT_OF_RANGE                => OffsetOutOfRangeException::class,
        self::CORRUPT_MESSAGE                    => CorruptMessageException::class,
        self::UNKNOWN_TOPIC_OR_PARTITION         => UnknownTopicOrPartitionException::class,
        self::INVALID_FETCH_SIZE                 => InvalidFetchSizeException::class,
        self::LEADER_NOT_AVAILABLE               => LeaderNotAvailableException::class,
        self::NOT_LEADER_FOR_PARTITION           => NotLeaderForPartitionException::class,
        self::REQUEST_TIMED_OUT                  => RequestTimedOutException::class,
        self::BROKER_NOT_AVAILABLE               => BrokerNotAvailableException::class,
        self::REPLICA_NOT_AVAILABLE              => ReplicaNotAvailableException::class,
        self::MESSAGE_SIZE_TOO_LARGE             => MessageSizeTooLargeException::class,
        self::STALE_CONTROLLER_EPOCH             => StaleControllerEpochException::class,
        self::OFFSET_METADATA_TOO_LARGE          => OffsetMetadataTooLargeException::class,
        self::STALE_LEADER_EPOCH                 => StaleLeaderEpochException::class,
        self::OFFSETS_LOAD_IN_PROGRESS           => OffsetsLoadInProgressException::class,
        self::CONSUMER_COORDINATOR_NOT_AVAILABLE => ConsumerCoordinatorNotAvailableException::class,
        self::NOT_COORDINATOR_FOR_CONSUMER       => NotCoordinatorForConsumerException::class,
        self::INVALID_TOPIC                      => InvalidTopicException::class,
        self::MESSAGE_SET_SIZE_TOO_LARGE         => MessageSetSizeTooLargeException::class,
        self::NOT_ENOUGH_REPLICAS                => NotEnoughReplicasException::class,
        self::NOT_ENOUGH_REPLICAS_AFTER_APPEND   => NotEnoughReplicasAfterAppendException::class,
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
    final public static function fromCode(int $errorCode, array $context = [], ?Exception $previous = null): self
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
     *
     * @return array
     */
    public function getContext(): array
    {
        return $this->context;
    }
}
