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

namespace Protocol\Kafka\Tests\Unit\Network;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\LeaderNotAvailableException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Network\RetryPolicy;

/**
 * Tests the classification of failures and the retry loop that is built on it
 */
#[CoversClass(RetryPolicy::class)]
final class RetryPolicyTest extends TestCase
{
    public function testOnlyTheErrorsThatAMetadataRefreshCanFixAreRetried(): void
    {
        self::assertSame(
            [
                KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
                KafkaException::LEADER_NOT_AVAILABLE,
                KafkaException::NOT_LEADER_FOR_PARTITION,
            ],
            RetryPolicy::RETRIABLE_ERROR_CODES
        );
    }

    /**
     * @return iterable<string, array{\Throwable, bool}>
     */
    public static function errorProvider(): iterable
    {
        yield 'UnknownTopicOrPartition (3)' => [new UnknownTopicOrPartitionException(), true];
        yield 'LeaderNotAvailable (5)'      => [new LeaderNotAvailableException(), true];
        yield 'NotLeaderForPartition (6)'   => [new NotLeaderForPartitionException(), true];
        yield 'dropped connection'          => [new NetworkException(), true];
        yield 'OffsetOutOfRange (1)'        => [new OffsetOutOfRangeException(), false];
        yield 'desynchronized connection'   => [new CorrelationIdMismatchException(), false];
        yield 'any other failure'           => [new \RuntimeException('boom'), false];
    }

    #[DataProvider('errorProvider')]
    public function testFailuresAreClassified(\Throwable $error, bool $isRetriable): void
    {
        self::assertSame($isRetriable, RetryPolicy::isRetriable($error));
    }

    public function testAPartialFailureIsRetriableOnlyWhenEveryPartitionIs(): void
    {
        $retriable = new TopicPartitionRequestException(
            ['orders' => [0 => 'ok']],
            ['orders' => [1 => new NotLeaderForPartitionException(), 2 => new NetworkException()]]
        );
        $permanent = new TopicPartitionRequestException(
            [],
            ['orders' => [1 => new NotLeaderForPartitionException(), 2 => new OffsetOutOfRangeException()]]
        );

        self::assertTrue(RetryPolicy::isRetriable($retriable));
        self::assertFalse(RetryPolicy::isRetriable($permanent));
    }

    public function testConfigurationDrivesTheNumberOfAttemptsAndTheBackoff(): void
    {
        $policy = RetryPolicy::fromConfiguration([
            ClientConfig::RETRIES          => 3,
            ClientConfig::RETRY_BACKOFF_MS => 25,
        ]);

        self::assertSame(3, $policy->getRetries());
        self::assertSame(25, $policy->getBackoffMs());
        self::assertSame(4, $policy->getMaxAttempts(), 'the first attempt is not a retry');
    }

    public function testAMissingConfigurationDoesNotRetryAtAll(): void
    {
        $policy = RetryPolicy::fromConfiguration([]);

        self::assertSame(0, $policy->getRetries());
        self::assertSame(1, $policy->getMaxAttempts());
        self::assertSame(100, $policy->getBackoffMs());
    }

    public function testANegativeNumberOfRetriesIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new RetryPolicy(-1);
    }

    public function testAnOperationThatSucceedsIsRunExactlyOnce(): void
    {
        $attempts = 0;
        $result   = new RetryPolicy(3, 0)->execute(function (int $attempt) use (&$attempts): string {
            $attempts = $attempt;

            return 'done';
        });

        self::assertSame('done', $result);
        self::assertSame(1, $attempts);
    }

    public function testARetriableFailureIsRepeatedUntilItSucceeds(): void
    {
        $refreshes = 0;
        $result    = new RetryPolicy(3, 0)->execute(
            static fn(int $attempt): string => $attempt < 3
                ? throw new NotLeaderForPartitionException(['attempt' => $attempt])
                : "succeeded on attempt {$attempt}",
            static function () use (&$refreshes): void {
                $refreshes++;
            }
        );

        self::assertSame('succeeded on attempt 3', $result);
        self::assertSame(2, $refreshes, 'the metadata is refreshed before each retry, not after the last attempt');
    }

    public function testTheLastFailureIsReportedOnceTheRetriesAreExhausted(): void
    {
        $attempts = 0;
        $policy   = new RetryPolicy(2, 0);

        try {
            $policy->execute(static function (int $attempt) use (&$attempts): never {
                $attempts = $attempt;

                throw new NetworkException(['attempt' => $attempt]);
            });
            self::fail('The exhausted policy is expected to report the last failure');
        } catch (NetworkException $exception) {
            self::assertSame(3, $attempts, 'one attempt plus two retries');
            self::assertSame(3, $exception->getContext()['attempt']);
        }
    }

    public function testAPermanentFailureIsNotRepeated(): void
    {
        $attempts = 0;

        $this->expectException(OffsetOutOfRangeException::class);

        try {
            new RetryPolicy(5, 0)->execute(static function (int $attempt) use (&$attempts): never {
                $attempts = $attempt;

                throw new OffsetOutOfRangeException();
            });
        } finally {
            self::assertSame(1, $attempts);
        }
    }

    public function testTheBackoffIsWaitedBetweenTwoAttempts(): void
    {
        $startedAt = microtime(true);

        new RetryPolicy(1, 40)->execute(
            static fn(int $attempt): string => $attempt === 1 ? throw new NetworkException() : 'ok'
        );

        self::assertGreaterThanOrEqual(0.03, microtime(true) - $startedAt);
    }
}
