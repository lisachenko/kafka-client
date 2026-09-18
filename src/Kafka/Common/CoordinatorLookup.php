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
use UnexpectedValueException;

/**
 * Discovers the broker that coordinates a consumer group or a transactional id (api key 10, FindCoordinator in 0.11).
 *
 * The lookup has to be retried: a broker creates the internal topic the coordinator lives in lazily, when the first
 * coordinator request that needs it arrives - `__consumer_offsets` for a group and `__transaction_state` for a
 * transactional id - and answers that first request, and every request that arrives while the topic is being
 * created, with the error code 15 (GroupCoordinatorNotAvailable). Error code 14 (GroupLoadInProgress) means the
 * coordinator is still reading the state out of that topic and is just as temporary. Both are retried with
 * `retry.backoff.ms` between the attempts, until the given timeout runs out.
 *
 * **The lookup is sent as version 4** (KIP-699, Kafka 3.0), which carries an array of `coordinator_keys` and
 * answers one entry per key: {@see self::findCoordinators()} looks several keys of one type up in a single round
 * trip, and {@see self::findCoordinator()} is the same request with a batch of one. The coordinator *type* is a
 * single field in front of the array and holds for every key of the batch, which is why a batch never mixes groups
 * and transactional ids ({@see GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION}); a type the broker does not
 * know is refused with the error code 42 (InvalidRequest) - per key, since version 4 - which this class turns into
 * the exception of that code.
 *
 * A batch is retried as a whole while **any** of its keys answers 14 or 15, because the api has no way to ask for
 * the rest of a batch only; the keys that are already answered are answered again, which costs nothing but the
 * bytes.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v5)"
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
        return $this->findCoordinators([$key], $coordinatorType, $timeoutMs)[$key];
    }

    /**
     * Returns the coordinator node of every given key, in one request (version 4, KIP-699, Kafka 3.0)
     *
     * Every key of the batch is looked up as `$coordinatorType`, and every one of them is reported with an error
     * code of its own: the first key that carries one fails the whole call, because a caller that asked for three
     * coordinators has no use for two of them. Duplicate keys are asked for once.
     *
     * @param list<string> $keys            Consumer group ids, or transactional ids of producers
     * @param int          $coordinatorType One of the `COORDINATOR_TYPE_*` constants of {@see GroupCoordinatorRequest}
     * @param int|null     $timeoutMs       How long to keep retrying, `metadata.fetch.timeout.ms` by default
     *
     * @return array<string, Node> The coordinator of every key, indexed by that key
     *
     * @throws Errors\GroupCoordinatorNotAvailableException If a coordinator did not become available in time
     * @throws Errors\GroupLoadInProgressException If a coordinator kept loading the state of its key
     * @throws Errors\InvalidRequestException If the broker does not know the coordinator type that was asked for
     * @throws AllBrokersNotAvailableException If not a single broker of the cluster answered
     * @throws NotCoordinatorForGroupException If the cluster does not know a node the broker pointed at
     */
    public function findCoordinators(
        array $keys,
        int $coordinatorType = GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
        ?int $timeoutMs = null
    ): array {
        $keys = array_values(array_unique($keys));
        if ($keys === []) {
            return [];
        }

        $timeoutMs ??= (int) ($this->configuration[ClientConfig::METADATA_FETCH_TIMEOUT_MS] ?? 0);
        $backoffMs   = (int) ($this->configuration[ClientConfig::RETRY_BACKOFF_MS] ?? 100);
        $deadline    = microtime(true) + $timeoutMs / 1000;

        $lastException = null;
        do {
            $response = $this->requestCoordinator($keys, $coordinatorType, $lastException);
            if (!self::isRetriableAnswer($response, $keys)) {
                break;
            }
            usleep($backoffMs * 1000);
        } while (microtime(true) < $deadline);

        if ($response === null) {
            throw new AllBrokersNotAvailableException(
                [
                    'groupId' => implode(', ', $keys),
                    'error'   => 'No broker of the cluster answered the coordinator request',
                ],
                KafkaException::UNKNOWN,
                $lastException
            );
        }

        $coordinators = [];
        foreach ($keys as $key) {
            $coordinators[$key] = $this->nodeOf($response, $key, $coordinatorType);
        }

        return $coordinators;
    }

    /**
     * Whether the answer asks for another attempt: a broker that did not answer at all, or a key that is not ready
     *
     * @param list<string> $keys
     */
    private static function isRetriableAnswer(?GroupCoordinatorResponse $response, array $keys): bool
    {
        if ($response === null) {
            return true;
        }

        foreach ($keys as $key) {
            try {
                $errorCode = $response->coordinatorOf($key)->errorCode;
            } catch (UnexpectedValueException) {
                // A batched answer that leaves a key out is not going to grow one on the next attempt; the
                // caller is told about it by the same exception out of self::nodeOf()
                return false;
            }
            if (in_array($errorCode, self::RETRIABLE_ERROR_CODES, true)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Turns the entry of one key into the node of the cluster it names
     *
     * @throws NotCoordinatorForGroupException If the cluster does not know the node the broker pointed at
     */
    private function nodeOf(GroupCoordinatorResponse $response, string $key, int $coordinatorType): Node
    {
        $entry = $response->coordinatorOf($key);
        if ($entry->errorCode !== KafkaException::NO_ERROR) {
            // The `error_message` of version 1 is the human readable half of the code and is passed on as context;
            // a version 4 answer of a 3.9.2 node leaves it empty, which is nothing to report
            throw KafkaException::fromCode(
                $entry->errorCode,
                array_filter(
                    [
                        'groupId'         => $key,
                        'coordinatorType' => $coordinatorType,
                        'errorMessage'    => $entry->errorMessage,
                    ],
                    static fn(mixed $value): bool => $value !== null && $value !== ''
                )
            );
        }

        try {
            $coordinator = $this->cluster->nodeById($entry->nodeId);
        } catch (Exception $exception) {
            throw new NotCoordinatorForGroupException(
                ['groupId' => $key, 'nodeId' => $entry->nodeId],
                KafkaException::NOT_COORDINATOR_FOR_GROUP,
                $exception
            );
        }
        if ($coordinator === null) {
            throw new NotCoordinatorForGroupException(
                [
                    'groupId' => $key,
                    'nodeId'  => $entry->nodeId,
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
     * @param list<string>   $keys            Names of the consumer groups, or transactional ids
     * @param int            $coordinatorType What to look the keys up as
     * @param Exception|null $lastException   Transport error of the last broker that was tried, if any
     */
    private function requestCoordinator(
        array $keys,
        int $coordinatorType,
        ?Exception &$lastException = null
    ): ?GroupCoordinatorResponse {
        $clientId = (string) ($this->configuration[ClientConfig::CLIENT_ID] ?? '');

        foreach ($this->cluster->nodes() as $node) {
            try {
                $stream        = $node->getConnection($this->configuration);
                $correlationId = AbstractRequest::nextCorrelationId();
                GroupCoordinatorRequest::forKeys($keys, $coordinatorType, $clientId, $correlationId)
                    ->writeTo($stream);

                return ResponseValidator::read(
                    GroupCoordinatorResponse::class,
                    $stream,
                    $correlationId,
                    ['groupId' => implode(', ', $keys), 'node' => $node->nodeId]
                );
            } catch (Exception $exception) {
                $lastException = $exception;
            }
        }

        return null;
    }
}
