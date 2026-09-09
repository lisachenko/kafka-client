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

namespace Protocol\Kafka\Tests\Unit\Common\Errors;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\BrokerNotAvailableException;
use Protocol\Kafka\Common\Errors\ClientExceptionInterface;
use Protocol\Kafka\Common\Errors\ClusterAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\ConcurrentTransactionsException;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Errors\DuplicateSequenceException;
use Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\IllegalSaslStateException;
use Protocol\Kafka\Common\Errors\InconsistentGroupProtocolException;
use Protocol\Kafka\Common\Errors\InvalidCommitOffsetSizeException;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\InvalidFetchSizeException;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Common\Errors\InvalidPartitionsException;
use Protocol\Kafka\Common\Errors\InvalidPidMappingException;
use Protocol\Kafka\Common\Errors\InvalidReplicaAssignmentException;
use Protocol\Kafka\Common\Errors\InvalidReplicationFactorException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidRequiredAcksException;
use Protocol\Kafka\Common\Errors\InvalidSessionTimeoutException;
use Protocol\Kafka\Common\Errors\InvalidTimestampException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\InvalidTxnStateException;
use Protocol\Kafka\Common\Errors\InvalidTxnTimeoutException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\LeaderNotAvailableException;
use Protocol\Kafka\Common\Errors\MessageTooLargeException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\NotControllerException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\NotEnoughReplicasAfterAppendException;
use Protocol\Kafka\Common\Errors\NotEnoughReplicasException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\OffsetMetadataTooLargeException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\OperationNotAttemptedException;
use Protocol\Kafka\Common\Errors\OutOfOrderSequenceException;
use Protocol\Kafka\Common\Errors\PolicyViolationException;
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\RecordListTooLargeException;
use Protocol\Kafka\Common\Errors\ReplicaNotAvailableException;
use Protocol\Kafka\Common\Errors\RequestTimedOutException;
use Protocol\Kafka\Common\Errors\RetriableException;
use Protocol\Kafka\Common\Errors\SecurityDisabledException;
use Protocol\Kafka\Common\Errors\ServerExceptionInterface;
use Protocol\Kafka\Common\Errors\StaleControllerEpochException;
use Protocol\Kafka\Common\Errors\TopicAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\TopicExistsException;
use Protocol\Kafka\Common\Errors\TransactionalIdAuthorizationException;
use Protocol\Kafka\Common\Errors\TransactionCoordinatorFencedException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Errors\UnsupportedForMessageFormatException;
use Protocol\Kafka\Common\Errors\UnsupportedSaslMechanismException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use RuntimeException;

/**
 * Verifies that the error codes of this branch are exactly the ones of Kafka 0.9.0.1.
 *
 * The codes -1 to 31 mirror clients/src/main/java/org/apache/kafka/common/protocol/Errors.java at tag 0.9.0.1, which is
 * the first release that carries the whole mapping in the Java client; the names of the codes 14-16 and 22-25 still
 * speak of *consumers* in kafka/common/ErrorMapping.scala. The class names are those of the later protocol lines
 * (branch main) so that the cascade merge stays small, and the retriable flags follow the RetriableException
 * hierarchy of the Java client of 0.9.0.1 (InvalidMetadataException extends RetriableException, so the codes 3, 5,
 * 6 and 13 are retriable; none of the codes 21-31 is). The codes 32-44 are those of Kafka 0.10.0 to 0.10.2
 * (Errors.java @ 0.10.2.2); of them only 41 NotController is retriable. The codes 45-55 are those of Kafka 0.11
 * (Errors.java @ 0.11.0.3): the producer id, sequence and transaction codes of KIP-98 and the two ACL codes; none
 * of them extends RetriableException in the Java client (the transactional producer retries 51 on its own).
 */
#[CoversClass(KafkaException::class)]
final class KafkaExceptionTest extends TestCase
{
    /**
     * Complete mapping of the 0.9.0.1 protocol: code => [exception class, is retriable]
     *
     * @return array<string, array{int, class-string<KafkaException>, bool}>
     */
    public static function errorCodeProvider(): array
    {
        return [
            'Unknown'                        => [-1, UnknownErrorException::class, false],
            'OffsetOutOfRange'               => [1, OffsetOutOfRangeException::class, false],
            'InvalidMessage'                 => [2, CorruptMessageException::class, true],
            'UnknownTopicOrPartition'        => [3, UnknownTopicOrPartitionException::class, true],
            'InvalidFetchSize'               => [4, InvalidFetchSizeException::class, false],
            'LeaderNotAvailable'             => [5, LeaderNotAvailableException::class, true],
            'NotLeaderForPartition'          => [6, NotLeaderForPartitionException::class, true],
            'RequestTimedOut'                => [7, RequestTimedOutException::class, true],
            'BrokerNotAvailable'             => [8, BrokerNotAvailableException::class, false],
            'ReplicaNotAvailable'            => [9, ReplicaNotAvailableException::class, false],
            'MessageSizeTooLarge'            => [10, MessageTooLargeException::class, false],
            'StaleControllerEpoch'           => [11, StaleControllerEpochException::class, false],
            'OffsetMetadataTooLarge'         => [12, OffsetMetadataTooLargeException::class, false],
            'NetworkException'               => [13, NetworkException::class, true],
            'OffsetsLoadInProgress'          => [14, GroupLoadInProgressException::class, true],
            'ConsumerCoordinatorNotAvailable' => [15, GroupCoordinatorNotAvailableException::class, true],
            'NotCoordinatorForConsumer'      => [16, NotCoordinatorForGroupException::class, true],
            'InvalidTopic'                   => [17, InvalidTopicException::class, false],
            'MessageSetSizeTooLarge'         => [18, RecordListTooLargeException::class, false],
            'NotEnoughReplicas'              => [19, NotEnoughReplicasException::class, true],
            'NotEnoughReplicasAfterAppend'   => [20, NotEnoughReplicasAfterAppendException::class, true],
            'InvalidRequiredAcks'            => [21, InvalidRequiredAcksException::class, false],
            'IllegalGeneration'              => [22, IllegalGenerationException::class, false],
            'InconsistentGroupProtocol'      => [23, InconsistentGroupProtocolException::class, false],
            'InvalidGroupId'                 => [24, InvalidGroupIdException::class, false],
            'UnknownMemberId'                => [25, UnknownMemberIdException::class, false],
            'InvalidSessionTimeout'          => [26, InvalidSessionTimeoutException::class, false],
            'RebalanceInProgress'            => [27, RebalanceInProgressException::class, false],
            'InvalidCommitOffsetSize'        => [28, InvalidCommitOffsetSizeException::class, false],
            'TopicAuthorizationFailed'       => [29, TopicAuthorizationFailedException::class, false],
            'GroupAuthorizationFailed'       => [30, GroupAuthorizationFailedException::class, false],
            'ClusterAuthorizationFailed'     => [31, ClusterAuthorizationFailedException::class, false],
            'InvalidTimestamp'              => [32, InvalidTimestampException::class, false],
            'UnsupportedSaslMechanism'      => [33, UnsupportedSaslMechanismException::class, false],
            'IllegalSaslState'              => [34, IllegalSaslStateException::class, false],
            'UnsupportedVersion'            => [35, UnsupportedVersionException::class, false],
            'TopicExists'                   => [36, TopicExistsException::class, false],
            'InvalidPartitions'             => [37, InvalidPartitionsException::class, false],
            'InvalidReplicationFactor'      => [38, InvalidReplicationFactorException::class, false],
            'InvalidReplicaAssignment'      => [39, InvalidReplicaAssignmentException::class, false],
            'InvalidConfig'                 => [40, InvalidConfigException::class, false],
            'NotController'                 => [41, NotControllerException::class, true],
            'InvalidRequest'                => [42, InvalidRequestException::class, false],
            'UnsupportedForMessageFormat'   => [43, UnsupportedForMessageFormatException::class, false],
            'PolicyViolation'               => [44, PolicyViolationException::class, false],
            'OutOfOrderSequenceNumber'      => [45, OutOfOrderSequenceException::class, false],
            'DuplicateSequenceNumber'       => [46, DuplicateSequenceException::class, false],
            'InvalidProducerEpoch'          => [47, ProducerFencedException::class, false],
            'InvalidTxnState'               => [48, InvalidTxnStateException::class, false],
            'InvalidProducerIdMapping'      => [49, InvalidPidMappingException::class, false],
            'InvalidTransactionTimeout'     => [50, InvalidTxnTimeoutException::class, false],
            'ConcurrentTransactions'        => [51, ConcurrentTransactionsException::class, false],
            'TransactionCoordinatorFenced'  => [52, TransactionCoordinatorFencedException::class, false],
            'TransactionalIdAuthorizationFailed' => [53, TransactionalIdAuthorizationException::class, false],
            'SecurityDisabled'              => [54, SecurityDisabledException::class, false],
            'OperationNotAttempted'         => [55, OperationNotAttemptedException::class, false],
        ];
    }

    #[DataProvider('errorCodeProvider')]
    public function testFromCodeCreatesTheExceptionOfThatCode(int $errorCode, string $expectedClass): void
    {
        $exception = KafkaException::fromCode($errorCode, ['topic' => 'test-topic']);

        self::assertInstanceOf($expectedClass, $exception);
        self::assertInstanceOf(RuntimeException::class, $exception);
    }

    #[DataProvider('errorCodeProvider')]
    public function testExceptionKeepsTheProtocolErrorCode(int $errorCode, string $expectedClass): void
    {
        $exception = KafkaException::fromCode($errorCode, []);

        self::assertSame($errorCode, $exception->getCode());
        self::assertSame($errorCode, new $expectedClass()->getCode());
    }

    #[DataProvider('errorCodeProvider')]
    public function testRetriableFlagMatchesTheJavaClient(int $errorCode, string $expectedClass, bool $isRetriable): void
    {
        $exception = KafkaException::fromCode($errorCode, []);

        self::assertSame($isRetriable, $exception instanceof RetriableException);
    }

    /**
     * Everything that fromCode() produces came from a broker response
     */
    #[DataProvider('errorCodeProvider')]
    public function testBrokerErrorsAreMarkedAsServerExceptions(int $errorCode): void
    {
        $exception = KafkaException::fromCode($errorCode, []);

        self::assertInstanceOf(ServerExceptionInterface::class, $exception);
        self::assertNotInstanceOf(ClientExceptionInterface::class, $exception);
    }

    #[DataProvider('errorCodeProvider')]
    public function testExceptionCarriesTheContextAndADescription(int $errorCode): void
    {
        $context   = ['topic' => 'test-topic', 'partitionId' => 7];
        $exception = KafkaException::fromCode($errorCode, $context);

        self::assertSame($context, $exception->getContext());
        self::assertStringContainsString('"topic":"test-topic"', $exception->getMessage());
        self::assertNotSame('', trim($exception->getMessage()));
    }

    /**
     * Codes above 55 were introduced by Kafka 1.0 and later (56 KAFKA_STORAGE_ERROR), a 0.11.0.3 broker never sends them
     *
     * @return array<string, array{int}>
     */
    public static function unmappedErrorCodeProvider(): array
    {
        return [
            'NoError'                       => [0],
            'KafkaStorageError (56)'        => [56],
            'LogDirNotFound (57)'           => [57],
            'out of range'                  => [4242],
            'negative out of range'         => [-999],
        ];
    }

    #[DataProvider('unmappedErrorCodeProvider')]
    public function testUnmappedCodesFallBackToUnknownError(int $errorCode): void
    {
        $exception = KafkaException::fromCode($errorCode, ['topic' => 'test-topic']);

        self::assertInstanceOf(UnknownErrorException::class, $exception);
        self::assertSame(KafkaException::UNKNOWN, $exception->getCode());
        self::assertSame(
            ['errorCode' => $errorCode, 'topic' => 'test-topic'],
            $exception->getContext()
        );
    }

    public function testPreviousExceptionIsChained(): void
    {
        $previous  = new \Exception('Connection reset by peer');
        $exception = KafkaException::fromCode(KafkaException::LEADER_NOT_AVAILABLE, [], $previous);

        self::assertSame($previous, $exception->getPrevious());
    }

    /**
     * Guards against a post-0.11 error class sneaking into the mapping
     */
    public function testOnlyTheErrorCodesOfKafka01103AreMapped(): void
    {
        $mappedCodes = [];
        foreach (range(-10, 60) as $errorCode) {
            $exception = KafkaException::fromCode($errorCode, []);
            if (!$exception instanceof UnknownErrorException || $errorCode === KafkaException::UNKNOWN) {
                $mappedCodes[] = $errorCode;
            }
        }

        self::assertSame(array_merge([-1], range(1, 55)), $mappedCodes);
    }

    /**
     * Code 13 was StaleLeaderEpoch in the 0.8 line and never left the broker; from 0.9 it is NetworkException,
     * which the socket layer of this client raises for a locally dropped connection as well
     */
    public function testNetworkExceptionCarriesTheWireCodeOfKafka09(): void
    {
        $exception = new NetworkException(['error' => 'Can not read from the stream']);

        self::assertSame(KafkaException::NETWORK_EXCEPTION, $exception->getCode());
        self::assertSame(13, $exception->getCode());
        self::assertInstanceOf(RetriableException::class, $exception);
        self::assertNotInstanceOf(ClientExceptionInterface::class, $exception);
        self::assertInstanceOf(NetworkException::class, KafkaException::fromCode(13, []));
    }
}
