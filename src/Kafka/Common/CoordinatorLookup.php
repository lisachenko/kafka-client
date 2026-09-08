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
 * Discovers the broker that holds the committed offsets of a consumer group (api key 10, ConsumerMetadata in 0.8.2).
 *
 * The lookup has to be retried: a 0.8.2.2 broker creates the internal `__consumer_offsets` topic lazily, when the
 * first coordinator request for any group arrives, and answers that first request - and every request that arrives
 * while the topic is being created - with the error code 15 (GroupCoordinatorNotAvailable). Error code 14
 * (GroupLoadInProgress) means the coordinator is still reading the offsets of the group out of that topic and is
 * just as temporary. Both are retried with `retry.backoff.ms` between the attempts, until the given timeout runs out.
 *
 * This mirrors the retry behaviour of `AdminClient::findCoordinator()` on branch `main`, which the 0.8 line has no
 * AdminClient for; the group membership APIs that would use it (api keys 11-14) only arrived in Kafka 0.9.
 *
 * @see docs/protocol/0.8.2.md, section "GroupCoordinator API (key 10, v0)"
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
     * Returns the coordinator node of the given consumer group
     *
     * @param string   $groupId   Name of the consumer group
     * @param int|null $timeoutMs How long to keep retrying, `metadata.fetch.timeout.ms` by default
     *
     * @throws Errors\GroupCoordinatorNotAvailableException If the coordinator did not become available in time
     * @throws Errors\GroupLoadInProgressException If the coordinator kept loading the offsets of the group
     * @throws AllBrokersNotAvailableException If not a single broker of the cluster answered
     * @throws NotCoordinatorForGroupException If the cluster does not know the node the broker pointed at
     */
    public function findCoordinator(string $groupId, ?int $timeoutMs = null): Node
    {
        $timeoutMs ??= (int) ($this->configuration[ClientConfig::METADATA_FETCH_TIMEOUT_MS] ?? 0);
        $backoffMs   = (int) ($this->configuration[ClientConfig::RETRY_BACKOFF_MS] ?? 100);
        $deadline    = microtime(true) + $timeoutMs / 1000;

        $lastException = null;
        do {
            $response          = $this->requestCoordinator($groupId, $lastException);
            $errorCode         = $response?->errorCode ?? KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE;
            $isRetriableAnswer = in_array($errorCode, self::RETRIABLE_ERROR_CODES, true);
            if (!$isRetriableAnswer) {
                break;
            }
            usleep($backoffMs * 1000);
        } while (microtime(true) < $deadline);

        if ($response === null) {
            throw new AllBrokersNotAvailableException(
                ['groupId' => $groupId, 'error' => 'No broker of the cluster answered the coordinator request'],
                KafkaException::UNKNOWN,
                $lastException
            );
        }
        if ($errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($errorCode, ['groupId' => $groupId]);
        }

        try {
            $coordinator = $this->cluster->nodeById($response->coordinator->nodeId);
        } catch (Exception $exception) {
            throw new NotCoordinatorForGroupException(
                ['groupId' => $groupId, 'nodeId' => $response->coordinator->nodeId],
                KafkaException::NOT_COORDINATOR_FOR_GROUP,
                $exception
            );
        }
        if ($coordinator === null) {
            throw new NotCoordinatorForGroupException(
                [
                    'groupId' => $groupId,
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
     * @param string         $groupId       Name of the consumer group
     * @param Exception|null $lastException Transport error of the last broker that was tried, if any
     */
    private function requestCoordinator(string $groupId, ?Exception &$lastException = null): ?GroupCoordinatorResponse
    {
        $clientId = (string) ($this->configuration[ClientConfig::CLIENT_ID] ?? '');

        foreach ($this->cluster->nodes() as $node) {
            try {
                $stream        = $node->getConnection($this->configuration);
                $correlationId = AbstractRequest::nextCorrelationId();
                new GroupCoordinatorRequest($groupId, $clientId, $correlationId)->writeTo($stream);

                return ResponseValidator::read(
                    GroupCoordinatorResponse::class,
                    $stream,
                    $correlationId,
                    ['groupId' => $groupId, 'node' => $node->nodeId]
                );
            } catch (Exception $exception) {
                $lastException = $exception;
            }
        }

        return null;
    }
}
