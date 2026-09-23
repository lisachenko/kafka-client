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

namespace Protocol\Kafka\Tests\Fixture;

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;

/**
 * Waits until a Kafka broker has published its metadata cache.
 *
 * A broker that has just booted answers a Metadata request with an EMPTY broker array: the alive-broker set of its
 * metadata cache is only filled once the controller has pushed an UpdateMetadata request to it, and on a cluster
 * without a single topic that never happens. Asking for a topic that does not exist yet unblocks it, because
 * `auto.create.topics.enable` makes the broker create that topic and elect a leader for it.
 *
 * An empty broker array is therefore "not ready, retry", never "the cluster has no brokers".
 *
 * The group coordinator of a fresh node is not ready either: `__consumer_offsets` is created by the first
 * FindCoordinator the node receives, and until its partitions are loaded every group request is answered with 15
 * (`CoordinatorNotAvailable`), 14 (`CoordinatorLoadInProgress`) or 16 (`NotCoordinator`) - a load that takes
 * milliseconds on an idle node and seconds on a loaded CI runner. {@see self::awaitGroupCoordinator()} warms it up
 * once per process, so that the first group test of a run does not pay for the creation of the internal topic.
 *
 * The broker probe deliberately stays on VERSION 0 of the Metadata api, the one every Kafka release from 0.8 on serves:
 * it is the very first frame the suite sends to a broker that may still be booting, and its answer
 * ({@see ClusterMetadataResponse}) only has to say whether a broker was advertised. Nothing of what the later
 * versions add - the cluster id, the controller id, the racks - is of any use before the cluster is up.
 *
 * @see docs/protocol/4.3.md, section "Cluster readiness"
 */
final class ClusterReadinessProbe
{
    public const string PROBE_TOPIC_PREFIX = 'kafka-client-readiness';

    /**
     * The codes the group apis answer while `__consumer_offsets` is being created or loaded
     */
    private const array COORDINATOR_NOT_READY = [
        KafkaException::GROUP_LOAD_IN_PROGRESS,
        KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
        KafkaException::NOT_COORDINATOR_FOR_GROUP,
    ];

    /**
     * Number of requests that the last call to awaitBrokers() or awaitGroupCoordinator() needed
     */
    private int $attempts = 0;

    /**
     * @param \Closure(): Stream $streamFactory       Opens a fresh connection to the broker
     * @param float              $timeout             How long to keep retrying, in seconds
     * @param int                $backoffMicroseconds How long to wait between two attempts
     */
    public function __construct(
        private readonly \Closure $streamFactory,
        private readonly float $timeout = 60.0,
        private readonly int $backoffMicroseconds = 250000,
        private readonly string $clientId = self::PROBE_TOPIC_PREFIX,
    ) {}

    /**
     * Returns the advertised brokers as soon as the cluster publishes them
     *
     * @return array<int, BrokerRecord>
     */
    public function awaitBrokers(?string $probeTopic = null): array
    {
        $probeTopic ??= self::PROBE_TOPIC_PREFIX . '-' . bin2hex(random_bytes(6));
        $deadline     = microtime(true) + $this->timeout;

        $this->attempts = 0;
        do {
            $this->attempts++;
            try {
                $stream = ($this->streamFactory)();
                new MetadataRequestV0([$probeTopic], $this->clientId, $this->attempts)->writeTo($stream);

                $response = ClusterMetadataResponse::unpack($stream);
                if ($response->brokers !== []) {
                    return $response->brokers;
                }
            } catch (KafkaException) {
                // The broker is not accepting connections yet, or dropped this one while it was still starting up
            }
            usleep($this->backoffMicroseconds);
        } while (microtime(true) < $deadline);

        throw new \RuntimeException(
            "The cluster did not publish any broker within {$this->timeout} seconds ({$this->attempts} attempts)"
        );
    }

    /**
     * Waits until the group coordinator of the cluster answers a group request with a decision
     *
     * A FindCoordinator for a group id the cluster never saw makes the node create `__consumer_offsets` and answers
     * 15 until the topic exists; an OffsetFetch of that group sent afterwards answers 14 or 16 until the coordinator
     * has loaded the partition of the group. Any other answer - the code 0 of a group without offsets, or a refusal
     * of its own - is the coordinator at work, and what a test that joins, commits or lists a group can rely on.
     *
     * The probe only tells that the coordinator is up; it never leaves a group behind, because an OffsetFetch
     * writes nothing.
     *
     * @throws \RuntimeException If the coordinator does not answer within the timeout
     */
    public function awaitGroupCoordinator(?string $probeGroup = null): void
    {
        $probeGroup ??= self::PROBE_TOPIC_PREFIX . '-' . bin2hex(random_bytes(6));
        $deadline     = microtime(true) + $this->timeout;

        $this->attempts = 0;
        do {
            $this->attempts++;
            try {
                $stream = ($this->streamFactory)();
                new GroupCoordinatorRequest(
                    $probeGroup,
                    GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
                    $this->clientId,
                    $this->attempts
                )->writeTo($stream);
                $coordinator = GroupCoordinatorResponse::unpack($stream)->coordinatorOf($probeGroup);

                if ($coordinator->errorCode === KafkaException::NO_ERROR) {
                    $stream = ($this->streamFactory)();
                    new OffsetFetchRequest($probeGroup, null, $this->clientId, $this->attempts)->writeTo($stream);
                    $errorCode = OffsetFetchResponse::unpack($stream)->groupOf($probeGroup)->errorCode;

                    if (!in_array($errorCode, self::COORDINATOR_NOT_READY, true)) {
                        return;
                    }
                } elseif (!in_array($coordinator->errorCode, self::COORDINATOR_NOT_READY, true)) {
                    throw KafkaException::fromCode($coordinator->errorCode, ['groupId' => $probeGroup]);
                }
            } catch (KafkaException $e) {
                // The broker dropped the connection while it was still starting up; a refusal of its own is final
                if ($e->getCode() !== 0 && !in_array($e->getCode(), self::COORDINATOR_NOT_READY, true)) {
                    throw $e;
                }
            }
            usleep($this->backoffMicroseconds);
        } while (microtime(true) < $deadline);

        throw new \RuntimeException(
            "The group coordinator did not answer within {$this->timeout} seconds ({$this->attempts} attempts)"
        );
    }

    /**
     * Returns how many requests the last call needed
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }
}
