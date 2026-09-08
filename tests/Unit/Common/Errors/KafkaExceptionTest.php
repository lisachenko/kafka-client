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
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\InvalidFetchSizeException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\LeaderNotAvailableException;
use Protocol\Kafka\Common\Errors\MessageTooLargeException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\NotEnoughReplicasAfterAppendException;
use Protocol\Kafka\Common\Errors\NotEnoughReplicasException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\OffsetMetadataTooLargeException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\RecordListTooLargeException;
use Protocol\Kafka\Common\Errors\ReplicaNotAvailableException;
use Protocol\Kafka\Common\Errors\RequestTimedOutException;
use Protocol\Kafka\Common\Errors\RetriableException;
use Protocol\Kafka\Common\Errors\ServerExceptionInterface;
use Protocol\Kafka\Common\Errors\StaleControllerEpochException;
use Protocol\Kafka\Common\Errors\StaleLeaderEpochException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use RuntimeException;

/**
 * Verifies that the error codes of this branch are exactly the ones of Kafka 0.8.2.2.
 *
 * The codes mirror kafka/common/ErrorMapping.scala at tag 0.8.2.2, while the class names are those of the later
 * protocol lines (branch main) so that the cascade merge stays small; the retriable flags follow the
 * RetriableException hierarchy of the Java client.
 */
#[CoversClass(KafkaException::class)]
final class KafkaExceptionTest extends TestCase
{
    /**
     * Complete mapping of the 0.8.2.2 protocol: code => [exception class, is retriable]
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
            'StaleLeaderEpoch'               => [13, StaleLeaderEpochException::class, false],
            'OffsetsLoadInProgress'          => [14, GroupLoadInProgressException::class, true],
            'ConsumerCoordinatorNotAvailable' => [15, GroupCoordinatorNotAvailableException::class, true],
            'NotCoordinatorForConsumer'      => [16, NotCoordinatorForGroupException::class, true],
            'InvalidTopic'                   => [17, InvalidTopicException::class, false],
            'MessageSetSizeTooLarge'         => [18, RecordListTooLargeException::class, false],
            'NotEnoughReplicas'              => [19, NotEnoughReplicasException::class, true],
            'NotEnoughReplicasAfterAppend'   => [20, NotEnoughReplicasAfterAppendException::class, true],
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
     * Codes above 20 were introduced by Kafka 0.9 and later, a 0.8.2.2 broker never sends them
     *
     * @return array<string, array{int}>
     */
    public static function unmappedErrorCodeProvider(): array
    {
        return [
            'NoError'                  => [0],
            'InvalidRequiredAcks (21)' => [21],
            'UnknownMemberId (25)'     => [25],
            'UnsupportedVersion (35)'  => [35],
            'out of range'             => [4242],
            'negative out of range'    => [-999],
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
     * Guards against a post-0.8 error class sneaking back into the mapping
     */
    public function testOnlyTheErrorCodesOfKafka0802AreMapped(): void
    {
        $mappedCodes = [];
        foreach (range(-10, 60) as $errorCode) {
            $exception = KafkaException::fromCode($errorCode, []);
            if (!$exception instanceof UnknownErrorException || $errorCode === KafkaException::UNKNOWN) {
                $mappedCodes[] = $errorCode;
            }
        }

        self::assertSame(array_merge([-1], range(1, 20)), $mappedCodes);
    }

    /**
     * On 0.8.2 code 13 is StaleLeaderEpoch; NetworkException is client-side here and carries no wire code
     */
    public function testNetworkExceptionIsAClientSideErrorOutsideTheCodeMap(): void
    {
        $exception = new NetworkException(['error' => 'Can not read from the stream']);

        self::assertInstanceOf(ClientExceptionInterface::class, $exception);
        self::assertInstanceOf(RetriableException::class, $exception);
        self::assertNotInstanceOf(NetworkException::class, KafkaException::fromCode(13, []));
        self::assertInstanceOf(StaleLeaderEpochException::class, KafkaException::fromCode(13, []));
    }
}
