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

namespace Protocol\Kafka\Network;

use Closure;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\LeaderNotAvailableException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Throwable;

/**
 * Decides which failures are worth another attempt and how long to wait in between.
 *
 * Almost every transient failure of a 0.8 cluster is a symptom of metadata that the client cached before a leader
 * moved: the request reaches a broker that does not host the partition (any more), and it answers with
 *
 *   - 3 UnknownTopicOrPartition - this broker does not know the topic-partition at all, e.g. because the topic was
 *     only just auto-created and the controller has not published it to every broker yet,
 *   - 5 LeaderNotAvailable - the partition currently has no leader, an election is in progress,
 *   - 6 NotLeaderForPartition - the broker hosts the partition, but it is a follower and not the leader.
 *
 * A dropped connection ({@see NetworkException}) has the same cure. All four are answered by refreshing the cluster
 * metadata and sending the request again, up to `retries` times with `retry.backoff.ms` in between; every other
 * error is final and is reported to the caller straight away.
 *
 * @see docs/protocol/0.11.0.md, section "Error codes"
 */
final class RetryPolicy
{
    /**
     * Wire error codes that a metadata refresh can fix
     *
     * @var list<int>
     */
    public const array RETRIABLE_ERROR_CODES = [
        KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
        KafkaException::LEADER_NOT_AVAILABLE,
        KafkaException::NOT_LEADER_FOR_PARTITION,
    ];

    /**
     * Exception classes that are worth another attempt
     *
     * @var list<class-string<Throwable>>
     */
    private const array RETRIABLE_EXCEPTIONS = [
        UnknownTopicOrPartitionException::class,
        LeaderNotAvailableException::class,
        NotLeaderForPartitionException::class,
        NetworkException::class,
    ];

    /**
     * @param int $retries   How many additional attempts a failed request gets
     * @param int $backoffMs How long to wait before each of them, in milliseconds
     */
    public function __construct(private readonly int $retries = 0, private readonly int $backoffMs = 100)
    {
        if ($retries < 0) {
            throw new \InvalidArgumentException("The number of retries can not be negative, {$retries} given");
        }
    }

    /**
     * Builds the policy out of the `retries` and `retry.backoff.ms` options of the client configuration
     *
     * @param array<string, mixed> $configuration Client configuration
     */
    public static function fromConfiguration(array $configuration): self
    {
        return new self(
            (int) ($configuration[ClientConfig::RETRIES] ?? 0),
            (int) ($configuration[ClientConfig::RETRY_BACKOFF_MS] ?? 100)
        );
    }

    /**
     * Checks whether the given failure can be fixed by refreshing the metadata and trying again.
     *
     * A {@see TopicPartitionRequestException} is retriable when every single topic-partition of it is: a request that
     * partially failed for a permanent reason is reported to the caller with the partial result it did produce.
     */
    public static function isRetriable(Throwable $error): bool
    {
        if ($error instanceof TopicPartitionRequestException) {
            foreach ($error->getExceptions() as $partitions) {
                foreach ($partitions as $partitionError) {
                    if (!self::isRetriable($partitionError)) {
                        return false;
                    }
                }
            }

            return true;
        }

        foreach (self::RETRIABLE_EXCEPTIONS as $retriableClass) {
            if ($error instanceof $retriableClass) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns how many additional attempts a failed request gets
     */
    public function getRetries(): int
    {
        return $this->retries;
    }

    /**
     * Returns how long to wait between two attempts, in milliseconds
     */
    public function getBackoffMs(): int
    {
        return $this->backoffMs;
    }

    /**
     * Returns the total number of attempts a request gets, the first one included
     */
    public function getMaxAttempts(): int
    {
        return $this->retries + 1;
    }

    /**
     * Waits for `retry.backoff.ms` before the next attempt
     */
    public function backoff(): void
    {
        if ($this->backoffMs > 0) {
            usleep($this->backoffMs * 1000);
        }
    }

    /**
     * Runs the operation and repeats it while it fails with a retriable error
     *
     * @template T
     *
     * @param Closure(int): T          $operation   Receives the number of the attempt, starting at 1
     * @param null|Closure(Throwable): void $beforeRetry Called before each retry, e.g. to refresh the metadata
     *
     * @return T
     */
    public function execute(Closure $operation, ?Closure $beforeRetry = null): mixed
    {
        for ($attempt = 1;; $attempt++) {
            try {
                return $operation($attempt);
            } catch (Throwable $error) {
                if ($attempt >= $this->getMaxAttempts() || !self::isRetriable($error)) {
                    throw $error;
                }
                if ($beforeRetry !== null) {
                    $beforeRetry($error);
                }
                $this->backoff();
            }
        }
    }
}
