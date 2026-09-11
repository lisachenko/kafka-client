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

namespace Protocol\Kafka\Common;

use Exception;
use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;

/**
 * Discovers the broker that coordinates a consumer group or a transactional id (api key 10, FindCoordinator in 0.11).
 *
 * The lookup has to be retried: a 0.11.0.3 broker creates the internal topic the coordinator lives in lazily, when
 * the first coordinator request that needs it arrives - `__consumer_offsets` for a group and `__transaction_state`
 * for a transactional id - and answers that first request, and every request that arrives while the topic is being
 * created, with the error code 15 (GroupCoordinatorNotAvailable). Error code 14 (GroupLoadInProgress) means the
 * coordinator is still reading the state out of that topic and is just as temporary. Both are retried with
 * `retry.backoff.ms` between the attempts, until the given timeout runs out.
 *
 * The lookup is sent as **version 1** of the api, which is what makes the transaction type reachable at all
 * ({@see GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION}); an unknown coordinator type is refused by the
 * broker with the error code 42 (InvalidRequest) and an `error_message`, which this class turns into the exception
 * of that code.
 *
 * This mirrors the retry behaviour of `AdminClient::findCoordinator()` on branch `main`, which the 0.8 line has no
 * AdminClient for; the group membership APIs that would use it (api keys 11-14) only arrived in Kafka 0.9.
 *
 * @see docs/protocol/2.8.md, section "GroupCoordinator API (key 10, v0 to v2)"
 */
final class CoordinatorLookup
{
    /**
     * Error codes that mean "the coordinator is not ready yet", as opposed to "there is no coordinator"
     */
    private const array RETRIABLE_ERROR_CODES = [
        KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
        KafkaException::GROUP_LOAD_IN_PROGRESS,
    ];

    /**
     * @param Cluster              $cluster       Cluster to look the coordinator up in
     * @param array<string, mixed> $configuration Client configuration
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly array $configuration = []
    ) {}

    /**
     * Returns the coordinator node of the given consumer group or transactional id
     *
     * @param string   $key             Name of the consumer group, or the transactional id of a producer
     * @param int      $coordinatorType One of the `COORDINATOR_TYPE_*` constants of {@see GroupCoordinatorRequest}
     * @param int|null $timeoutMs       How long to keep retrying, `metadata.fetch.timeout.ms` by default
     *
     * @throws Errors\GroupCoordinatorNotAvailableException If the coordinator did not become available in time
     * @throws Errors\GroupLoadInProgressException If the coordinator kept loading the state of the key
     * @throws Errors\InvalidRequestException If the broker does not know the coordinator type that was asked for
     * @throws AllBrokersNotAvailableException If not a single broker of the cluster answered
     * @throws NotCoordinatorForGroupException If the cluster does not know the node the broker pointed at
     */
    public function findCoordinator(
        string $key,
        int $coordinatorType = GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
        ?int $timeoutMs = null
    ): Node {
        $timeoutMs ??= (int) ($this->configuration[ClientConfig::METADATA_FETCH_TIMEOUT_MS] ?? 0);
        $backoffMs   = (int) ($this->configuration[ClientConfig::RETRY_BACKOFF_MS] ?? 100);
        $deadline    = microtime(true) + $timeoutMs / 1000;

        $lastException = null;
        do {
            $response          = $this->requestCoordinator($key, $coordinatorType, $lastException);
            $errorCode         = $response?->errorCode ?? KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE;
            $isRetriableAnswer = in_array($errorCode, self::RETRIABLE_ERROR_CODES, true);
            if (!$isRetriableAnswer) {
                break;
            }
            usleep($backoffMs * 1000);
        } while (microtime(true) < $deadline);

        if ($response === null) {
            throw new AllBrokersNotAvailableException(
                ['groupId' => $key, 'error' => 'No broker of the cluster answered the coordinator request'],
                KafkaException::UNKNOWN,
                $lastException
            );
        }
        if ($errorCode !== KafkaException::NO_ERROR) {
            // The `error_message` of version 1 is the human readable half of the code and is passed on as context
            throw KafkaException::fromCode(
                $errorCode,
                array_filter(
                    [
                        'groupId'         => $key,
                        'coordinatorType' => $coordinatorType,
                        'errorMessage'    => $response->errorMessage,
                    ],
                    static fn(mixed $value): bool => $value !== null
                )
            );
        }

        try {
            $coordinator = $this->cluster->nodeById($response->coordinator->nodeId);
        } catch (Exception $exception) {
            throw new NotCoordinatorForGroupException(
                ['groupId' => $key, 'nodeId' => $response->coordinator->nodeId],
                KafkaException::NOT_COORDINATOR_FOR_GROUP,
                $exception
            );
        }
        if ($coordinator === null) {
            throw new NotCoordinatorForGroupException(
                [
                    'groupId' => $key,
                    'nodeId'  => $response->coordinator->nodeId,
                    'error'   => 'The cluster does not know the node the broker pointed at',
                ],
                KafkaException::NOT_COORDINATOR_FOR_GROUP
            );
        }

        return $coordinator;
    }

    /**
     * Sends the coordinator request to the brokers of the cluster, until one of them answers
     *
     * @param string         $key             Name of the consumer group, or a transactional id
     * @param int            $coordinatorType What to look the key up as
     * @param Exception|null $lastException   Transport error of the last broker that was tried, if any
     */
    private function requestCoordinator(
        string $key,
        int $coordinatorType,
        ?Exception &$lastException = null
    ): ?GroupCoordinatorResponse {
        $clientId = (string) ($this->configuration[ClientConfig::CLIENT_ID] ?? '');

        foreach ($this->cluster->nodes() as $node) {
            try {
                $stream        = $node->getConnection($this->configuration);
                $correlationId = AbstractRequest::nextCorrelationId();
                new GroupCoordinatorRequest($key, $coordinatorType, $clientId, $correlationId)->writeTo($stream);

                return ResponseValidator::read(
                    GroupCoordinatorResponse::class,
                    $stream,
                    $correlationId,
                    ['groupId' => $key, 'node' => $node->nodeId]
                );
            } catch (Exception $exception) {
                $lastException = $exception;
            }
        }

        return null;
    }
}
