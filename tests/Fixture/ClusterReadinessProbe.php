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
use Protocol\Kafka\Protocol\Request\MetadataRequestV0;

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
 * The probe deliberately stays on VERSION 0 of the Metadata api, the one every Kafka release from 0.8 on serves:
 * it is the very first frame the suite sends to a broker that may still be booting, and its answer
 * ({@see ClusterMetadataResponse}) only has to say whether a broker was advertised. Nothing of what the later
 * versions add - the cluster id, the controller id, the racks - is of any use before the cluster is up.
 *
 * @see docs/protocol/1.1.md, section "Cluster readiness"
 */
final class ClusterReadinessProbe
{
    public const string PROBE_TOPIC_PREFIX = 'kafka-client-readiness';

    /**
     * Number of Metadata requests that the last call to awaitBrokers() needed
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
     * Returns how many Metadata requests the last call needed
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }
}
