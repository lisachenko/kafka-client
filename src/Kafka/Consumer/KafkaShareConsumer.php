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

namespace Protocol\Kafka\Consumer;

use Closure;
use InvalidArgumentException;
use LogicException;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Common\Errors\InvalidShareSessionEpochException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\ShareSessionLimitReachedException;
use Protocol\Kafka\Common\Errors\ShareSessionNotFoundException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Serialization\Deserializer;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\Consumer\Internals\ShareInFlightBatch;
use Protocol\Kafka\Consumer\Internals\ShareMembershipManager;
use Protocol\Kafka\Consumer\Internals\ShareSessionHandler;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;
use Protocol\Kafka\Protocol\Data\ShareFetchResponsePartition;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeResponse;
use Protocol\Kafka\Protocol\Request\ShareFetchRequest;
use Protocol\Kafka\Protocol\Request\ShareFetchResponse;
use Throwable;

/**
 * A client that consumes records from a Kafka cluster as a member of a **share group** (KIP-932, Kafka 4.1).
 *
 * `KafkaShareConsumer` of the Java client @ 4.3.1, with the surface of its `ShareConsumer` interface adapted to this
 * package's {@see KafkaConsumer}. The members of a share group do not own partitions, they **share** them record by
 * record: every record of a subscribed topic is delivered to one member at a time, which holds it under an
 * **acquisition lock** (the group config `share.record.lock.duration.ms`, 30000 by default) until it acknowledges it:
 *
 * - {@see AcknowledgeType::ACCEPT}: processed, never delivered again;
 * - {@see AcknowledgeType::RELEASE}: delivered again, to this member or another one, with a delivery count one higher,
 *   until the group config `share.delivery.count.limit` (5) archives it;
 * - {@see AcknowledgeType::REJECT}: archived at once, never delivered again;
 * - {@see AcknowledgeType::RENEW}: still being processed - the lock starts over and the next poll() returns the
 *   record again (Kafka 4.2, KIP-1222);
 * - nothing: the lock expires and the record is delivered again.
 *
 * Usage against a broker on 127.0.0.1:9092, see also examples/share-consumer.php:
 *
 * ```php
 * $consumer = new KafkaShareConsumer([
 *     ConsumerConfig::BOOTSTRAP_SERVERS          => ['tcp://127.0.0.1:9092'],
 *     ConsumerConfig::GROUP_ID                   => 'my-share-group',
 *     ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT,
 * ]);
 * $consumer->subscribe(['my-topic']);
 *
 * while (true) {
 *     foreach ($consumer->poll(1000) as $topic => $partitions) {
 *         foreach ($partitions as $partition => $records) {
 *             foreach ($records as $record) {
 *                 $consumer->acknowledge($record, process($record) ? AcknowledgeType::ACCEPT : AcknowledgeType::REJECT);
 *             }
 *         }
 *     }
 *     $consumer->commitSync();
 * }
 * $consumer->close();
 * ```
 *
 * **Acknowledgement modes.** `share.acknowledgement.mode` ({@see ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE}) is
 * `implicit` by default: every record a poll() returned is accepted by the next poll(), commitSync() or commitAsync(),
 * and {@see self::acknowledge()} is refused. In the `explicit` mode the application acknowledges every record itself,
 * and a poll() refuses to run while a record of the last one has no acknowledgement. The acknowledgements travel in
 * the ShareFetch of the next poll(), or in a ShareAcknowledge of {@see self::commitSync()} and
 * {@see self::commitAsync()}; {@see self::setAcknowledgementCommitCallback()} observes their outcome.
 *
 * **What happens on the wire.** The first poll() joins the group with ShareGroupHeartbeat (key 76) under a member id
 * the consumer generated for itself, every poll() heartbeats at the interval the coordinator dictates
 * ({@see ShareMembershipManager}), and the records come from a **share session** on the leader of each assigned
 * partition ({@see ShareSessionHandler}): a ShareFetch (key 78) with the epoch 0 opens it, every further one carries
 * the next epoch, the acknowledgements and the partitions that came or went, and {@see self::close()} closes it with
 * a ShareAcknowledge (key 79) of the epoch -1. A session the node lost - it lives on its connection, so a connection
 * that dropped takes it along - is answered 122 `ShareSessionNotFound` or 123 `InvalidShareSessionEpoch`, and the
 * consumer opens a new one with the epoch 0: the acknowledgements of the lost session are reported as failed to the
 * callback, and the records it held are delivered again.
 *
 * **PHP has no background thread**, so everything the Java consumer does in its network thread happens inside the
 * calls of the application: the heartbeat is sent from poll(), acknowledgements are sent when poll(), a commit or
 * close() is called, and the acknowledgement commit callback runs inside that call. An application that does not poll
 * for `group.share.session.timeout.ms` (45000 on the broker) is dropped from its group, and its next poll() joins it
 * again; one that does not poll within the acquisition lock loses its records to the other members.
 *
 * **The group decides where to start and what to show.** A share group reads from `latest` unless its group config
 * `share.auto.offset.reset` says `earliest`, and its isolation level is the group config `share.isolation.level`;
 * the consumer options for both - and every other option of a classic or KIP-848 consumer a share consumer cannot use -
 * are refused by the constructor, as `ShareConsumerConfig` @ 4.3.1 refuses them
 * ({@see ConsumerConfig::SHARE_GROUP_UNSUPPORTED_CONFIGS}).
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
class KafkaShareConsumer
{
    /**
     * The consumer configs
     *
     * @var array<string, mixed>
     */
    private readonly array $configuration;

    /**
     * Kafka cluster configuration, resolved on the first use
     */
    private ?Cluster $cluster = null;

    /**
     * Low-level kafka client, created on the first use
     */
    private ?Client $client = null;

    /**
     * Topics this consumer subscribed to
     *
     * @var list<string>
     */
    private array $subscription = [];

    /**
     * Membership of the share group, created by the first poll()
     */
    private ?ShareMembershipManager $membership = null;

    /**
     * Member id of this consumer, generated once and kept for its whole life (KIP-932, as KIP-1082 does it)
     */
    private readonly string $memberId;

    /**
     * Share session of this member on each leader it fetched from, by node id
     *
     * @var array<int, ShareSessionHandler>
     */
    private array $sessions = [];

    /**
     * Records this member holds and what the application said about them, as [topic][partition] => batch
     *
     * @var array<string, array<int, ShareInFlightBatch>>
     */
    private array $inFlightBatches = [];

    /**
     * Whether the records in flight were acquired by a poll() that threw before it returned them
     */
    private bool $inFlightHeldBack = false;

    /**
     * Acquisition lock timeout of the last answer that named one, null before the first
     */
    private ?int $acquisitionLockTimeoutMs = null;

    /**
     * Observer of the completed acknowledgements, if any
     */
    private ?AcknowledgementCommitCallback $acknowledgementCommitCallback = null;

    /**
     * Whether the acknowledgement commit callback is running, which makes the methods of this consumer inaccessible
     */
    private bool $inCallback = false;

    /**
     * Whether {@see self::close()} was called
     */
    private bool $closed = false;

    /**
     * Whether the application acknowledges every record itself (`share.acknowledgement.mode = explicit`)
     */
    private readonly bool $explicitAcknowledgement;

    /**
     * Acquire mode of every ShareFetch, the wire value of `share.acquire.mode`
     */
    private readonly int $shareAcquireMode;

    /**
     * `max.poll.records`, the `max_records` and the `batch_size` of every ShareFetch
     */
    private readonly int $maxPollRecords;

    /**
     * Deserializer for the record keys, or null to keep them as raw byte strings
     */
    private readonly ?Deserializer $keyDeserializer;

    /**
     * Deserializer for the record values, or null to keep them as raw byte strings
     */
    private readonly ?Deserializer $valueDeserializer;

    /**
     * @param array<string, mixed> $configuration Consumer options, see {@see ConsumerConfig}
     *
     * @throws InvalidConfigurationException For an option a share consumer cannot use, and for a value of
     *                                       `share.acknowledgement.mode`, `share.acquire.mode` or `max.poll.records`
     *                                       it does not know
     */
    public function __construct(array $configuration = [])
    {
        $unsupported = array_values(array_filter(
            ConsumerConfig::SHARE_GROUP_UNSUPPORTED_CONFIGS,
            static fn(string $option): bool => array_key_exists($option, $configuration)
        ));
        if ($unsupported !== []) {
            throw new InvalidConfigurationException(implode(', ', $unsupported) . ' cannot be set when using a share group.');
        }

        $this->configuration = $configuration + ConsumerConfig::getDefaultConfiguration();

        $this->explicitAcknowledgement = match (self::optionValue($this->configuration, ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE)) {
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_IMPLICIT => false,
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT => true,
            default => throw new InvalidConfigurationException(sprintf(
                'Invalid value for configuration %s. The value must either be \'%s\' or \'%s\'.',
                ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE,
                ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_IMPLICIT,
                ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT
            )),
        };
        $this->shareAcquireMode = match (self::optionValue($this->configuration, ConsumerConfig::SHARE_ACQUIRE_MODE)) {
            ConsumerConfig::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED => ShareFetchRequest::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED,
            ConsumerConfig::SHARE_ACQUIRE_MODE_RECORD_LIMIT    => ShareFetchRequest::SHARE_ACQUIRE_MODE_RECORD_LIMIT,
            default => throw new InvalidConfigurationException(sprintf(
                'Invalid value for configuration %s. The value must either be \'%s\' or \'%s\'.',
                ConsumerConfig::SHARE_ACQUIRE_MODE,
                ConsumerConfig::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED,
                ConsumerConfig::SHARE_ACQUIRE_MODE_RECORD_LIMIT
            )),
        };

        $maxPollRecords = $this->configuration[ConsumerConfig::MAX_POLL_RECORDS] ?? ConsumerConfig::DEFAULT_MAX_POLL_RECORDS;
        if (!is_numeric($maxPollRecords) || (int) $maxPollRecords < 1) {
            throw new InvalidConfigurationException(ConsumerConfig::MAX_POLL_RECORDS . ' must be at least 1.');
        }
        $this->maxPollRecords = (int) $maxPollRecords;

        $this->keyDeserializer   = self::resolveDeserializer($this->configuration[ConsumerConfig::KEY_DESERIALIZER]);
        $this->valueDeserializer = self::resolveDeserializer($this->configuration[ConsumerConfig::VALUE_DESERIALIZER]);
        $this->memberId          = ConsumerGroupHeartbeatCoordinator::newMemberId();
    }

    /**
     * Get the current subscription: the topics of the most recent {@see self::subscribe()}, or none
     *
     * @return list<string>
     */
    public function subscription(): array
    {
        $this->acquireAndEnsureOpen();

        return $this->subscription;
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     *
     * Topic subscriptions are not incremental: this list replaces the current one, and an empty list is treated the
     * same as {@see self::unsubscribe()}. Nothing is sent here; the next {@see self::poll()} joins the share group, or
     * - for a member that is in it already - sends the new subscription in its next heartbeat.
     *
     * @param list<string> $topics List of topics to subscribe to
     *
     * @throws InvalidArgumentException If a topic name is empty
     * @throws InvalidGroupIdException  If the consumer has no `group.id`
     */
    public function subscribe(array $topics): void
    {
        $this->acquireAndEnsureOpen();
        if ($topics === []) {
            $this->unsubscribe();

            return;
        }

        $topicNames = array_values(array_unique(array_map(strval(...), $topics)));
        if (array_filter($topicNames, static fn(string $topic): bool => trim($topic) === '') !== []) {
            throw new InvalidArgumentException('Topic collection to subscribe to cannot contain null or empty topic');
        }
        $this->requireGroupId();

        $this->subscription = $topicNames;
    }

    /**
     * Unsubscribe from the topics of {@see self::subscribe()}.
     *
     * The acknowledgements that were not sent yet go out with the close of the share sessions of this member - which
     * releases whatever it still holds -, and the member leaves its group with the epoch -1. The member id stays: a
     * consumer that subscribes again joins the group under it.
     */
    public function unsubscribe(): void
    {
        $this->acquireAndEnsureOpen();

        $firstError = $this->leave();
        $this->subscription = [];
        if ($firstError !== null) {
            throw $firstError;
        }
    }

    /**
     * Deliver records for the topics of {@see self::subscribe()}.
     *
     * One pass of a poll() heartbeats when the interval of the coordinator has elapsed (and joins the group the first
     * time), sends the acknowledgements of the records the last poll() returned to the leaders that acquired them,
     * and fetches from the leader of every assigned partition. It returns as soon as a pass acquired records, and waits
     * - in `fetch.max.wait.ms` steps, on the node - until the timeout is over otherwise.
     *
     * In the implicit mode the records of the last poll() are accepted first; in the explicit mode each of them has
     * to be acknowledged before. Records renewed with {@see AcknowledgeType::RENEW} are returned again, by the poll()
     * that sends the renewal or, after a commit that sent it, by the next one - without new records.
     *
     * @param int $timeout The time, in milliseconds, spent waiting in poll if data is not available
     *
     * @return array<string, array<int, list<ConsumerRecord>>> [topic][partition] => the acquired records, in offset
     *         order, each with its {@see ConsumerRecord::$deliveryCount}
     *
     * @throws InvalidArgumentException If the timeout is negative
     * @throws LogicException           If the consumer is not subscribed to any topics, or it uses the explicit
     *                                  acknowledgement and a record of the last poll() has no acknowledgement
     * @throws KafkaException           For an error of the group or of a fetch the consumer cannot recover from
     */
    public function poll(int $timeout): array
    {
        $this->acquireAndEnsureOpen();
        if ($timeout < 0) {
            throw new InvalidArgumentException('Timeout must not be negative');
        }

        if ($this->inFlightHeldBack) {
            // The poll() that acquired these records threw before it returned them: they are this poll()'s records
            $this->inFlightHeldBack = false;
            $records                = $this->inFlightRecords();
            if ($records !== []) {
                return $records;
            }
        }

        if ($this->explicitAcknowledgement) {
            foreach ($this->batches() as $batch) {
                if (!$batch->allInFlightAcknowledged()) {
                    throw new LogicException('All records must be acknowledged in explicit acknowledgement mode.');
                }
            }
        } else {
            $this->acknowledgeAllInFlight();
        }
        if ($this->subscription === []) {
            throw new LogicException('Consumer is not subscribed to any topics.');
        }

        $deadlineMs = self::nowMs() + $timeout;
        do {
            $membership = $this->membership();
            $membership->poll(self::nowMs(), $this->subscription);
            $assigned = $membership->assignedPartitions();

            // The node holds a ShareFetch for `fetch.max.wait.ms` at most, so the heartbeat is never late by more
            $maxWaitMs = min((int) $this->configuration[ConsumerConfig::FETCH_MAX_WAIT_MS], $deadlineMs - self::nowMs());
            $records   = $this->pollOnce($assigned, max(0, $maxWaitMs));
            if ($records !== []) {
                return $records;
            }

            $remainingMs = $deadlineMs - self::nowMs();
            if ($remainingMs > 0 && $assigned === []) {
                // Nothing to fetch from yet: the next heartbeat may bring the partitions
                usleep(min($remainingMs, ShareMembershipManager::UNASSIGNED_HEARTBEAT_INTERVAL_MS) * 1000);
            }
        } while (self::nowMs() < $deadlineMs);

        return [];
    }

    /**
     * Acknowledge delivery of a record returned by the last {@see self::poll()}.
     *
     * The two forms of the Java client share the name:
     *
     * ```php
     * $consumer->acknowledge($record);                                        // accept
     * $consumer->acknowledge($record, AcknowledgeType::RELEASE);
     * $consumer->acknowledge('my-topic', 0, 42, AcknowledgeType::REJECT);     // topic, partition, offset, type
     * ```
     *
     * The second form is for a record that is not available as a {@see ConsumerRecord} - one a deserializer failed on,
     * which the consumer has released already and the application may reject instead. The acknowledgement is sent by
     * the next poll(), commitSync(), commitAsync() or close(); a later acknowledgement of the same record before that
     * replaces it. This method can only be used in the **explicit** acknowledgement mode.
     *
     * @param ConsumerRecord|string $record  The record to acknowledge, or the topic of the offset to acknowledge
     * @param AcknowledgeType|int   $type    The acknowledge type, or the partition of the offset to acknowledge
     * @param int|null              $offset  The offset to acknowledge, in the second form
     * @param AcknowledgeType|null  $offsetType The acknowledge type, in the second form
     *
     * @throws LogicException           If the record is not waiting to be acknowledged, or the consumer is not using
     *                                  explicit acknowledgement
     * @throws InvalidArgumentException If the arguments are neither of the two forms
     */
    public function acknowledge(
        ConsumerRecord|string $record,
        AcknowledgeType|int $type = AcknowledgeType::ACCEPT,
        ?int $offset = null,
        ?AcknowledgeType $offsetType = null
    ): void {
        $this->acquireAndEnsureOpen();
        if (!$this->explicitAcknowledgement) {
            throw new LogicException('Implicit acknowledgement of delivery is being used.');
        }

        if ($record instanceof ConsumerRecord) {
            if (!$type instanceof AcknowledgeType || $offset !== null || $offsetType !== null) {
                throw new InvalidArgumentException('A record is acknowledged as acknowledge($record, $type).');
            }
            $batch = $this->inFlightBatches[$record->topic][$record->partition] ?? null;
            if ($batch === null || $record->offset === null) {
                throw new LogicException('The record cannot be acknowledged.');
            }
            $batch->acknowledge($record->offset, $type);

            return;
        }

        if (!is_int($type) || $offset === null || $offsetType === null) {
            throw new InvalidArgumentException('An offset is acknowledged as acknowledge($topic, $partition, $offset, $type).');
        }
        $batch = $this->inFlightBatches[$record][$type] ?? null;
        if ($batch === null || !$batch->isAcknowledgeable($offset)) {
            throw new LogicException('The record cannot be acknowledged.');
        }
        $batch->acknowledgeOffset($offset, $offsetType);
    }

    /**
     * Commit the acknowledgements of the records returned, and wait for the answer.
     *
     * In the explicit mode these are the acknowledgements given with {@see self::acknowledge()}; in the implicit mode
     * every record the last poll() returned is accepted. They are sent in a ShareAcknowledge (key 79) to each leader
     * that acquired records, in the share session of this member there. An acknowledgement that failed does not
     * throw: its exception is the entry of its partition, and so is the error of a request that failed as a whole.
     *
     * @return array<string, array<int, KafkaException|null>> [topic][partition] => null when the acknowledgements of
     *         that partition were committed, the exception of the node otherwise
     */
    public function commitSync(): array
    {
        $this->acquireAndEnsureOpen();
        if (!$this->explicitAcknowledgement) {
            $this->acknowledgeAllInFlight();
        }

        $results = [];
        foreach ($this->takeAcknowledgementsByNode() as $nodeId => $entries) {
            $results = array_replace_recursive($results, $this->commitNodeAcknowledgements($nodeId, $entries));
        }

        return $results;
    }

    /**
     * Commit the acknowledgements of the records returned, without reporting the outcome to the caller.
     *
     * What is sent is what {@see self::commitSync()} sends. PHP has no background thread, so the request is sent and
     * answered before this method returns, too; the difference is that nothing is thrown or returned - the outcome
     * reaches the acknowledgement commit callback, if one is set.
     */
    public function commitAsync(): void
    {
        $this->acquireAndEnsureOpen();
        if (!$this->explicitAcknowledgement) {
            $this->acknowledgeAllInFlight();
        }

        foreach ($this->takeAcknowledgementsByNode() as $nodeId => $entries) {
            try {
                $this->commitNodeAcknowledgements($nodeId, $entries);
            } catch (Throwable) {
                // An asynchronous commit reports through the callback only
            }
        }
    }

    /**
     * Sets the acknowledgement commit callback which can be used to handle acknowledgement completion.
     *
     * The callback is called once per topic-partition of every request that carried acknowledgements, with the error
     * the node answered, inside the call of this consumer that sent the request. Null removes it.
     *
     * @param AcknowledgementCommitCallback|Closure(array<string, array<int, list<int>>>, ?KafkaException): void|null $callback
     */
    public function setAcknowledgementCommitCallback(AcknowledgementCommitCallback|Closure|null $callback): void
    {
        $this->acquireAndEnsureOpen();
        if ($callback instanceof Closure) {
            $callback = new class ($callback) implements AcknowledgementCommitCallback {
                public function __construct(private readonly Closure $callback) {}

                public function onComplete(array $offsets, ?KafkaException $exception): void
                {
                    ($this->callback)($offsets, $exception);
                }
            };
        }
        $this->acknowledgementCommitCallback = $callback;
    }

    /**
     * Returns the acquisition lock timeout of the records fetched last, in milliseconds.
     *
     * The `acquisition_lock_timeout_ms` of the last ShareFetch answer that acquired records, or of the last
     * ShareAcknowledge answer (version 2 carries it, KIP-1222) - the group config `share.record.lock.duration.ms`.
     *
     * @return int|null The timeout, or null while no answer named one
     */
    public function acquisitionLockTimeoutMs(): ?int
    {
        $this->acquireAndEnsureOpen();

        return $this->acquisitionLockTimeoutMs;
    }

    /**
     * Close the consumer: acknowledge what is pending, close the share sessions and leave the group.
     *
     * The acknowledgements given and not sent yet travel with the ShareAcknowledge of the epoch -1 that closes the
     * share session on each leader, which **releases** every record this member still holds there - in the implicit
     * mode, the records of the last poll() are released rather than accepted, as in the Java client. The member then
     * leaves its group with the heartbeat of the epoch -1. Every step is attempted even if one fails, and the first
     * error is thrown at the end. A closed consumer refuses every further call.
     *
     * @throws KafkaException The first error of the shutdown
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->acquire();

        try {
            $firstError = $this->leave();
        } finally {
            $this->closed = true;
        }
        if ($firstError !== null) {
            throw $firstError;
        }
    }

    /**
     * A consumer that goes out of scope releases what it holds and its membership
     */
    public function __destruct()
    {
        if ($this->closed || $this->inCallback) {
            return;
        }

        try {
            $this->close();
        } catch (Throwable) {
            // The process is shutting down; a broker that is not reachable any more can not be helped here
        }
    }

    /**
     * Cluster lazy-loading
     */
    protected function getCluster(): Cluster
    {
        return $this->cluster ??= Cluster::bootstrap($this->configuration);
    }

    /**
     * Lazy-loading for the low-level kafka client
     */
    protected function getClient(): Client
    {
        return $this->client ??= new Client($this->getCluster(), $this->configuration);
    }

    /**
     * One pass of a poll(): the acknowledgements, the renewed records, and the fetch of new ones
     *
     * @param array<string, list<int>> $assigned  Partitions assigned to this member, by topic name
     * @param int                      $maxWaitMs How long the first fetch of the pass may wait for records
     *
     * @return array<string, array<int, list<ConsumerRecord>>>
     */
    private function pollOnce(array $assigned, int $maxWaitMs): array
    {
        $cluster          = $this->getCluster();
        $acknowledgements = $this->takeAcknowledgementsByNode();
        $renewedBefore    = $this->takeRenewedRecords();
        // Renewals first, new records afterwards, as `ShareConsumerImpl.collect()` @ 4.3.1: a pass that renews or
        // returns renewed records fetches nothing new
        $fetchNew = $renewedBefore === [] && !self::hasRenewals($acknowledgements);

        $targets = [];
        if ($fetchNew) {
            foreach ($assigned as $topic => $partitions) {
                $topicId = $cluster->topicIdOf((string) $topic);
                if ($topicId === null) {
                    continue;
                }
                foreach ($partitions as $partition) {
                    try {
                        $leader = $cluster->leaderFor((string) $topic, $partition);
                    } catch (KafkaException) {
                        continue;
                    }
                    $targets[$leader->nodeId][$topicId][] = $partition;
                }
            }
        }

        $nodeIds = array_unique(array_merge(
            array_keys($targets),
            array_keys($acknowledgements),
            array_keys(array_filter(
                $this->sessions,
                static fn(ShareSessionHandler $handler): bool => !$handler->isNewSession()
            ))
        ));
        $deserializationError = null;
        foreach ($nodeIds as $nodeId) {
            $entries    = $acknowledgements[$nodeId] ?? [];
            $partitions = $targets[$nodeId] ?? [];
            $handler    = $this->sessions[$nodeId] ??= new ShareSessionHandler($nodeId);
            $node       = $cluster->nodeById($nodeId);
            if ($node === null) {
                $handler->reset();
                $this->completeAcknowledgements($entries, static fn(): KafkaException => new ShareSessionNotFoundException(['nodeId' => $nodeId]));
                continue;
            }

            if (!$fetchNew) {
                // Only acknowledgements go to this leader: with the renewal flag in a ShareFetch that fetches nothing,
                // as `ShareSessionHandler` @ 4.3.1 sends them, or in a ShareAcknowledge
                if ($entries === []) {
                    continue;
                }
                if (!self::hasRenewals([$nodeId => $entries])) {
                    $this->commitNodeAcknowledgements($nodeId, $entries);
                    continue;
                }
                try {
                    $this->fetchFromNode($node, $handler, [], $entries, 0, true);
                } catch (ShareSessionNotFoundException | InvalidShareSessionEpochException | ShareSessionLimitReachedException) {
                    // The renewal failed with its session, which the callback was told; the next pass opens a new one
                }
                continue;
            }

            if ($partitions === []) {
                // Nothing to fetch from this leader any more: its session goes, with what is left to acknowledge
                $this->closeSession($node, $handler, $entries);
                continue;
            }

            // The acknowledgements of partitions this fetch does not keep in the session go ahead on their own
            $sessionPartitions = $handler->sessionPartitions();
            $withFetch         = [];
            $ahead             = [];
            foreach ($entries as $entry) {
                $batch = $entry[0];
                $kept  = in_array($batch->partition, $partitions[$batch->topicId] ?? [], true)
                    && in_array($batch->partition, $sessionPartitions[$batch->topicId] ?? [], true);
                if ($kept) {
                    $withFetch[] = $entry;
                } else {
                    $ahead[] = $entry;
                }
            }
            if ($ahead !== []) {
                $this->commitNodeAcknowledgements($nodeId, $ahead);
            }

            try {
                $deserializationError ??= $this->fetchFromNode($node, $handler, $partitions, $withFetch, $maxWaitMs, false);
            } catch (ShareSessionNotFoundException | InvalidShareSessionEpochException | ShareSessionLimitReachedException) {
                // The session is gone or out of step: the next pass opens a new one with the epoch 0
                continue;
            }
            $maxWaitMs = 0;
        }

        $this->takeRenewedRecords();
        if ($deserializationError !== null) {
            $this->inFlightHeldBack = true;

            throw $deserializationError;
        }

        return $this->inFlightRecords();
    }

    /**
     * Sends one ShareFetch to a leader: the partitions that came and went, the acknowledgements of its session, and
     * reads the records it acquired
     *
     * @param array<string, list<int>>                          $partitions Partitions to fetch, by raw topic id
     * @param list<array{0: ShareInFlightBatch, 1: array<int, int>}> $entries    Acknowledgements for this session
     * @param bool                                              $renewOnly  Whether the request only carries
     *        acknowledgements with a renewal, and fetches nothing
     *
     * @return Throwable|null The first error of a deserializer, whose record was released
     *
     * @throws ShareSessionNotFoundException|InvalidShareSessionEpochException|ShareSessionLimitReachedException When
     *         the session has to be opened again
     * @throws KafkaException For any other error of the request
     */
    private function fetchFromNode(
        Node $node,
        ShareSessionHandler $handler,
        array $partitions,
        array $entries,
        int $maxWaitMs,
        bool $renewOnly
    ): ?Throwable {
        if ($handler->isNewSession() && $entries !== []) {
            // Acknowledgements belong to the session that acquired the records, and a new session may carry none
            $this->completeAcknowledgements($entries, static fn(): KafkaException => new ShareSessionNotFoundException(['nodeId' => $node->nodeId]));
            $entries = [];
            if ($renewOnly) {
                return null;
            }
        }

        [$added, $forgotten] = $renewOnly ? [[], []] : $handler->prepareFetch($partitions);
        $isRenewAck          = self::hasRenewals([$node->nodeId => $entries]);

        try {
            $answer = $this->getClient()->shareFetch(
                $node,
                $this->requireGroupId(),
                $this->memberId,
                $handler->nextEpoch(),
                $added,
                self::wireAcknowledgements($entries),
                $maxWaitMs,
                (int) $this->configuration[ConsumerConfig::FETCH_MIN_BYTES],
                $this->maxPollRecords,
                $this->maxPollRecords,
                $forgotten,
                $this->shareAcquireMode,
                $isRenewAck
            );
        } catch (KafkaException $exception) {
            $handler->reset();
            $this->completeAcknowledgements($entries, static fn(): KafkaException => $exception);

            throw $exception;
        }
        $handler->handleResponse();

        $this->completeAcknowledgements(
            $entries,
            static function (ShareInFlightBatch $batch) use ($answer): ?KafkaException {
                $partition = $answer->partitionOf($batch->topicId, $batch->partition);
                if ($partition === null || $partition->acknowledgeErrorCode === KafkaException::NO_ERROR) {
                    return null;
                }

                return KafkaException::fromCode(
                    $partition->acknowledgeErrorCode,
                    self::errorContext($batch, $partition->acknowledgeErrorMessage)
                );
            }
        );
        if ($isRenewAck) {
            $this->acquisitionLockTimeoutMs = $answer->acquisitionLockTimeoutMs;
        }

        return $renewOnly ? null : $this->readAcquiredRecords($node, $answer);
    }

    /**
     * Turns the acquired records of a ShareFetch answer into records in flight
     *
     * An acquired offset that holds no record is acknowledged as a gap; a record a deserializer fails on is released,
     * and its error is handed back to be thrown once the answer is read.
     *
     * @throws KafkaException For a partition the fetch has no right to read
     */
    private function readAcquiredRecords(Node $node, ShareFetchResponse $answer): ?Throwable
    {
        $cluster              = $this->getCluster();
        $deserializationError = null;
        $acquiredAny          = false;
        foreach ($answer->responses as $topicResponse) {
            $topic = $cluster->topicNameById($topicResponse->topicId);
            foreach ($topicResponse->partitions as $partitionIndex => $partition) {
                if ($partition->errorCode !== KafkaException::NO_ERROR) {
                    $this->handleFetchError($topic, (int) $partitionIndex, $partition);
                    continue;
                }
                if ($partition->acquiredRecords === [] || $topic === null) {
                    continue;
                }

                $acquiredAny = true;
                $batch       = $this->batchFor($node->nodeId, $topic, $topicResponse->topicId, (int) $partitionIndex);
                $records     = $partition->acquiredRecords();
                foreach ($partition->acquiredRecords as $range) {
                    for ($offset = $range->firstOffset; $offset <= $range->lastOffset; $offset++) {
                        if (!isset($records[$offset])) {
                            $batch->addGap($offset);
                            continue;
                        }
                        try {
                            $batch->addRecord($this->consumerRecord($records[$offset]['record'], $topic, (int) $partitionIndex, $range->deliveryCount));
                        } catch (Throwable $exception) {
                            $batch->addFailedRecord($offset);
                            $deserializationError ??= $exception;
                        }
                    }
                }
            }
        }
        if ($acquiredAny) {
            $this->acquisitionLockTimeoutMs = $answer->acquisitionLockTimeoutMs;
        }

        return $deserializationError;
    }

    /**
     * Reacts to the error of the fetch half of a partition: a stale picture of the leadership is refreshed, and the
     * errors a refresh cannot cure reach the caller
     *
     * @throws KafkaException For an error of the partition that is not about its leadership or its existence
     */
    private function handleFetchError(?string $topic, int $partition, ShareFetchResponsePartition $answer): void
    {
        $exception = KafkaException::fromCode($answer->errorCode, [
            'topic'     => $topic,
            'partition' => $partition,
        ] + ($answer->errorMessage !== null && $answer->errorMessage !== '' ? ['error' => $answer->errorMessage] : []));

        switch ($answer->errorCode) {
            case KafkaException::NOT_LEADER_FOR_PARTITION:
            case KafkaException::LEADER_NOT_AVAILABLE:
            case KafkaException::UNKNOWN_TOPIC_OR_PARTITION:
            case KafkaException::UNKNOWN_TOPIC_ID:
            case KafkaException::FENCED_LEADER_EPOCH:
            case KafkaException::UNKNOWN_LEADER_EPOCH:
                $this->getCluster()->reload();

                return;
        }

        throw $exception;
    }

    /**
     * Sends the acknowledgements of one leader in a ShareAcknowledge of its session, and reports their outcome
     *
     * @param list<array{0: ShareInFlightBatch, 1: array<int, int>}> $entries
     *
     * @return array<string, array<int, KafkaException|null>> [topic][partition] => the outcome
     */
    private function commitNodeAcknowledgements(int $nodeId, array $entries): array
    {
        $handler = $this->sessions[$nodeId] ?? null;
        $node    = $this->getCluster()->nodeById($nodeId);
        if ($handler === null || $handler->isNewSession() || $node === null) {
            // A session cannot be opened with a ShareAcknowledge: the one that acquired the records is gone
            return $this->completeAcknowledgements(
                $entries,
                static fn(): KafkaException => new ShareSessionNotFoundException(['nodeId' => $nodeId])
            );
        }

        $isRenewAck = self::hasRenewals([$nodeId => $entries]);
        try {
            $answer = $this->getClient()->shareAcknowledge(
                $node,
                $this->requireGroupId(),
                $this->memberId,
                $handler->nextEpoch(),
                self::wireAcknowledgements($entries),
                $isRenewAck
            );
        } catch (KafkaException $exception) {
            $handler->reset();

            return $this->completeAcknowledgements($entries, static fn(): KafkaException => $exception);
        }
        $handler->handleResponse();
        $this->acquisitionLockTimeoutMs = $answer->acquisitionLockTimeoutMs;

        return $this->completeAcknowledgements($entries, self::acknowledgeErrorOf($answer));
    }

    /**
     * Closes the share session on a leader with the epoch -1, carrying the acknowledgements that are left
     *
     * A renewal has no meaning in the close - the close releases every record the member still holds -, so the
     * offsets acknowledged with RENEW are left out and released with the rest.
     *
     * @param list<array{0: ShareInFlightBatch, 1: array<int, int>}> $entries
     *
     * @throws KafkaException When the node refused the close
     */
    private function closeSession(Node $node, ShareSessionHandler $handler, array $entries): void
    {
        if ($handler->isNewSession()) {
            $this->completeAcknowledgements($entries, static fn(): KafkaException => new ShareSessionNotFoundException(['nodeId' => $node->nodeId]));

            return;
        }

        $closing = [];
        foreach ($entries as [$batch, $acknowledgements]) {
            $batch->completeRenewals(false);
            $acknowledgements = array_filter($acknowledgements, static fn(int $type): bool => $type !== AcknowledgeType::RENEW->value);
            if ($acknowledgements !== []) {
                $closing[] = [$batch, $acknowledgements];
            }
        }

        try {
            $answer = $this->getClient()->shareAcknowledge(
                $node,
                $this->requireGroupId(),
                $this->memberId,
                ShareFetchRequest::FINAL_EPOCH,
                self::wireAcknowledgements($closing)
            );
        } catch (KafkaException $exception) {
            $this->completeAcknowledgements($closing, static fn(): KafkaException => $exception);

            throw $exception;
        } finally {
            $handler->reset();
        }
        $this->completeAcknowledgements($closing, self::acknowledgeErrorOf($answer));
    }

    /**
     * Sends what is left to acknowledge with the close of every share session, and leaves the group
     *
     * @return Throwable|null The first error, after every step was attempted
     */
    private function leave(): ?Throwable
    {
        $firstError       = null;
        $acknowledgements = $this->takeAcknowledgementsByNode();
        foreach ($this->sessions as $nodeId => $handler) {
            $entries = $acknowledgements[$nodeId] ?? [];
            unset($acknowledgements[$nodeId]);
            if ($handler->isNewSession() && $entries === []) {
                continue;
            }

            try {
                $node = $this->getCluster()->nodeById($nodeId);
                if ($node === null) {
                    $handler->reset();
                    continue;
                }
                $this->closeSession($node, $handler, $entries);
            } catch (Throwable $exception) {
                $firstError ??= $exception;
            }
        }
        foreach ($acknowledgements as $nodeId => $entries) {
            $this->completeAcknowledgements($entries, static fn(): KafkaException => new ShareSessionNotFoundException(['nodeId' => $nodeId]));
        }
        $this->sessions         = [];
        $this->inFlightBatches  = [];
        $this->inFlightHeldBack = false;

        try {
            $this->membership?->leaveGroup();
        } catch (Throwable $exception) {
            $firstError ??= $exception;
        }

        return $firstError;
    }

    /**
     * Hands the acknowledgements of every batch over to the requests that carry them, grouped by the leader that
     * acquired the records
     *
     * @return array<int, list<array{0: ShareInFlightBatch, 1: array<int, int>}>> Node id => the batches and their
     *         acknowledgements, as offset => the wire id of the type
     */
    private function takeAcknowledgementsByNode(): array
    {
        $byNode = [];
        foreach ($this->batches() as $batch) {
            if ($batch->hasAcknowledgements()) {
                $byNode[$batch->nodeId][] = [$batch, $batch->takeAcknowledgements()];
            }
        }

        return $byNode;
    }

    /**
     * Moves the records whose renewal the node answered back into the flight
     *
     * @return list<ConsumerRecord> The records that came back
     */
    private function takeRenewedRecords(): array
    {
        $renewed = [];
        foreach ($this->batches() as $batch) {
            array_push($renewed, ...$batch->takeRenewedRecords());
        }

        return $renewed;
    }

    /**
     * Reports the outcome of acknowledgements to the callback, and returns it by topic and partition
     *
     * @param list<array{0: ShareInFlightBatch, 1: array<int, int>}>  $entries
     * @param Closure(ShareInFlightBatch): (KafkaException|null)       $errorOf The error of the acknowledgements of a
     *        batch, null when they were committed
     *
     * @return array<string, array<int, KafkaException|null>>
     */
    private function completeAcknowledgements(array $entries, Closure $errorOf): array
    {
        $results = [];
        foreach ($entries as [$batch, $acknowledgements]) {
            $exception = $errorOf($batch);
            $batch->completeRenewals($exception === null);
            $results[$batch->topic][$batch->partition] = $exception;

            $offsets = array_values(array_filter(
                array_keys($acknowledgements),
                static fn(int $offset): bool => $acknowledgements[$offset] !== ShareAcknowledgementBatch::GAP
            ));
            if ($offsets === [] || $this->acknowledgementCommitCallback === null) {
                continue;
            }

            $this->inCallback = true;
            try {
                $this->acknowledgementCommitCallback->onComplete([$batch->topic => [$batch->partition => $offsets]], $exception);
            } catch (Throwable) {
                // An exception of the callback is the application's, as the Java handler logs and swallows it
            } finally {
                $this->inCallback = false;
            }
        }

        return $results;
    }

    /**
     * Accepts every record in flight that has no acknowledgement yet, which is the implicit mode
     */
    private function acknowledgeAllInFlight(): void
    {
        foreach ($this->batches() as $batch) {
            $batch->acknowledgeAll(AcknowledgeType::ACCEPT);
        }
    }

    /**
     * Returns the records in flight, as poll() returns them
     *
     * @return array<string, array<int, list<ConsumerRecord>>>
     */
    private function inFlightRecords(): array
    {
        $records = [];
        foreach ($this->inFlightBatches as $topic => $partitions) {
            foreach ($partitions as $partition => $batch) {
                $inFlight = $batch->inFlightRecords();
                if ($inFlight !== []) {
                    $records[$topic][$partition] = $inFlight;
                }
            }
        }

        return $records;
    }

    /**
     * Returns every batch this consumer holds, dropping the ones that have nothing left
     *
     * @return list<ShareInFlightBatch>
     */
    private function batches(): array
    {
        $batches = [];
        foreach ($this->inFlightBatches as $topic => $partitions) {
            foreach ($partitions as $partition => $batch) {
                if ($batch->isEmpty()) {
                    unset($this->inFlightBatches[$topic][$partition]);
                    continue;
                }
                $batches[] = $batch;
            }
            if ($this->inFlightBatches[$topic] === []) {
                unset($this->inFlightBatches[$topic]);
            }
        }

        return $batches;
    }

    /**
     * Returns the batch of a topic-partition, a new one when it has none or when another leader acquired its records
     */
    private function batchFor(int $nodeId, string $topic, string $topicId, int $partition): ShareInFlightBatch
    {
        $batch = $this->inFlightBatches[$topic][$partition] ?? null;
        if ($batch === null || ($batch->nodeId !== $nodeId && $batch->isEmpty())) {
            $batch = $this->inFlightBatches[$topic][$partition] = new ShareInFlightBatch($nodeId, $topic, $topicId, $partition);
        }

        return $batch;
    }

    /**
     * Builds the record poll() returns: the deserialized key and value, and the delivery count of the acquisition
     */
    private function consumerRecord(Record $record, string $topic, int $partition, int $deliveryCount): ConsumerRecord
    {
        return ConsumerRecord::fromRecord(
            $record,
            $topic,
            $partition,
            $this->keyDeserializer !== null && $record->key !== null
                ? $this->keyDeserializer->deserialize($topic, $record->key)
                : $record->key,
            $this->valueDeserializer !== null && $record->value !== null
                ? $this->valueDeserializer->deserialize($topic, $record->value)
                : $record->value,
            $deliveryCount
        );
    }

    /**
     * Returns the membership of the share group, created on the first use
     */
    private function membership(): ShareMembershipManager
    {
        return $this->membership ??= new ShareMembershipManager(
            $this->getClient(),
            $this->getCluster(),
            $this->requireGroupId(),
            $this->memberId,
            self::rackOf($this->configuration)
        );
    }

    /**
     * Refuses a call of a closed consumer, and one from inside the acknowledgement commit callback
     *
     * @throws LogicException
     */
    private function acquireAndEnsureOpen(): void
    {
        $this->acquire();
        if ($this->closed) {
            throw new LogicException('This consumer has already been closed.');
        }
    }

    /**
     * Refuses a call from inside the acknowledgement commit callback, as the Java client does
     *
     * @throws LogicException
     */
    private function acquire(): void
    {
        if ($this->inCallback) {
            throw new LogicException(
                'KafkaShareConsumer methods are not accessible from user-defined acknowledgement commit callback.'
            );
        }
    }

    /**
     * Returns the configured share group, failing when there is none
     *
     * @throws InvalidGroupIdException
     */
    private function requireGroupId(): string
    {
        $groupId = (string) ($this->configuration[ConsumerConfig::GROUP_ID] ?? '');
        if ($groupId === '') {
            throw new InvalidGroupIdException([
                'error' => 'You must provide a valid ' . ConsumerConfig::GROUP_ID . ' in the consumer configuration.',
            ]);
        }

        return $groupId;
    }

    /**
     * Converts acknowledgements into the argument of {@see Client::shareFetch()} and {@see Client::shareAcknowledge()}
     *
     * @param list<array{0: ShareInFlightBatch, 1: array<int, int>}> $entries
     *
     * @return array<string, array<int, list<ShareAcknowledgementBatch>>> Raw topic id => partition => batches
     */
    private static function wireAcknowledgements(array $entries): array
    {
        $wire = [];
        foreach ($entries as [$batch, $acknowledgements]) {
            $wire[$batch->topicId][$batch->partition] = ShareInFlightBatch::acknowledgementBatches($acknowledgements);
        }

        return $wire;
    }

    /**
     * Tells whether one of the acknowledgements is a renewal
     *
     * @param array<int, list<array{0: ShareInFlightBatch, 1: array<int, int>}>> $acknowledgements By node id
     */
    private static function hasRenewals(array $acknowledgements): bool
    {
        foreach ($acknowledgements as $entries) {
            foreach ($entries as [, $types]) {
                if (ShareInFlightBatch::hasRenewAcknowledgement($types)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Returns the reader of the partition errors of a ShareAcknowledge answer
     *
     * @return Closure(ShareInFlightBatch): (KafkaException|null)
     */
    private static function acknowledgeErrorOf(ShareAcknowledgeResponse $answer): Closure
    {
        return static function (ShareInFlightBatch $batch) use ($answer): ?KafkaException {
            $partition = $answer->partitionOf($batch->topicId, $batch->partition);
            if ($partition === null || $partition->errorCode === KafkaException::NO_ERROR) {
                return null;
            }

            return KafkaException::fromCode($partition->errorCode, self::errorContext($batch, $partition->errorMessage));
        };
    }

    /**
     * Names the topic-partition of an acknowledgement error, with the message of the node when it sent one
     *
     * @return array<string, mixed>
     */
    private static function errorContext(ShareInFlightBatch $batch, ?string $message): array
    {
        $context = ['topic' => $batch->topic, 'partition' => $batch->partition];
        if ($message !== null && $message !== '') {
            $context['error'] = $message;
        }

        return $context;
    }

    /**
     * Reads a string option in lower case, as the validators of the Java client compare them
     *
     * @param array<string, mixed> $configuration
     */
    private static function optionValue(array $configuration, string $option): string
    {
        $value = $configuration[$option] ?? '';

        return is_scalar($value) ? strtolower(trim((string) $value)) : '';
    }

    /**
     * Returns the `client.rack` of this consumer, null when it names none
     *
     * @param array<string, mixed> $configuration
     */
    private static function rackOf(array $configuration): ?string
    {
        $rack = $configuration[ConsumerConfig::CLIENT_RACK] ?? null;

        return is_string($rack) && $rack !== '' ? $rack : null;
    }

    /**
     * Builds a deserializer out of the configured instance or class name
     *
     * @throws InvalidConfigurationException
     */
    private static function resolveDeserializer(mixed $deserializer): ?Deserializer
    {
        if ($deserializer === null || $deserializer instanceof Deserializer) {
            return $deserializer;
        }

        if (is_string($deserializer) && is_subclass_of($deserializer, Deserializer::class)) {
            return new $deserializer();
        }

        throw new InvalidConfigurationException(
            'A deserializer has to be an instance of ' . Deserializer::class . ' or the name of a class that '
            . 'implements it, ' . get_debug_type($deserializer) . ' given.'
        );
    }

    /**
     * Current time, in milliseconds
     */
    private static function nowMs(): int
    {
        return (int) (microtime(true) * 1e3);
    }
}
