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

namespace Protocol\Kafka\Consumer\Internals;

use Protocol\Kafka\Protocol\Request\ShareFetchRequest;

/**
 * The share session of a share consumer on one leader (KIP-932)
 *
 * `ShareSessionHandler` of the Java client @ 4.3.1: the fetch session of KIP-227 for share groups. The node keys a
 * share session by the group, the member and the connection; this handler keeps the two things the member has to know
 * about it - the **epoch** of its next request and the **partitions** the session holds - and computes what the next
 * ShareFetch names:
 *
 * ```
 *   epoch 0   every partition to fetch                  -> the node opens the session (a new one replaces an old one)
 *   epoch n   only the partitions added and forgotten   -> the node keeps the rest of the session
 *   epoch -1  (ShareAcknowledge)                        -> the node closes the session and releases what is held
 * ```
 *
 * Every answer without a top-level error moves the epoch on by one ({@see self::handleResponse()}). The three errors
 * of the session itself - **122** `ShareSessionNotFound` (the node lost the session, usually with the connection it
 * lived on), **123** `InvalidShareSessionEpoch` and **133** `ShareSessionLimitReached` - and a request that never got
 * an answer start over with the epoch 0 and every partition, which is `nextCloseExistingAttemptNew()` of the Java
 * client: {@see self::reset()}. The Java handler moves the epoch on after any other top-level error; this one starts
 * over after those as well, because the 4.3.1 node does not consume the epoch of a request it refuses with the 42 -
 * the next epoch would be the 123. The 4.3.1 node answers an epoch 0 of a member whose session it still holds by
 * replacing the session, and the records the member holds stay acquired to it.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1 and v2)"
 */
final class ShareSessionHandler
{
    /**
     * Epoch of the next request of this session, {@see ShareFetchRequest::INITIAL_EPOCH} for a session to open
     */
    private int $nextEpoch = ShareFetchRequest::INITIAL_EPOCH;

    /**
     * Partitions the node holds in the session, as the raw 16 bytes of the topic id => its partitions
     *
     * @var array<string, list<int>>
     */
    private array $sessionPartitions = [];

    /**
     * @param int $nodeId Leader this session lives on
     */
    public function __construct(public readonly int $nodeId) {}

    /**
     * Returns the epoch of the next request
     */
    public function nextEpoch(): int
    {
        return $this->nextEpoch;
    }

    /**
     * Tells whether the next request opens the session, i.e. has the epoch 0 and may carry no acknowledgement
     */
    public function isNewSession(): bool
    {
        return $this->nextEpoch === ShareFetchRequest::INITIAL_EPOCH;
    }

    /**
     * Returns the partitions the node holds in the session
     *
     * @return array<string, list<int>> Raw topic id => partitions
     */
    public function sessionPartitions(): array
    {
        return $this->sessionPartitions;
    }

    /**
     * Computes what the next ShareFetch names for the given partitions, and takes them as the session's
     *
     * A new session names every partition; a running one names the partitions it adds and forgets the ones that left.
     *
     * @param array<string, list<int>> $partitions Partitions to fetch, as the raw topic id => its partitions
     *
     * @return array{0: array<string, list<int>>, 1: array<string, list<int>>} The partitions to add, and the ones
     *         to forget, both as the raw topic id => partitions
     */
    public function prepareFetch(array $partitions): array
    {
        $partitions = self::normalized($partitions);
        if ($this->isNewSession()) {
            $this->sessionPartitions = $partitions;

            return [$partitions, []];
        }

        $added     = self::difference($partitions, $this->sessionPartitions);
        $forgotten = self::difference($this->sessionPartitions, $partitions);

        $this->sessionPartitions = $partitions;

        return [$added, $forgotten];
    }

    /**
     * Moves the epoch on after an answer of the node that was not an error of the session
     */
    public function handleResponse(): void
    {
        $this->nextEpoch = $this->nextEpoch >= 0x7fffffff ? 1 : $this->nextEpoch + 1;
    }

    /**
     * Starts over with the epoch 0 and every partition, after a session error or a request without an answer
     */
    public function reset(): void
    {
        $this->nextEpoch         = ShareFetchRequest::INITIAL_EPOCH;
        $this->sessionPartitions = [];
    }

    /**
     * Sorts the partitions by topic id and partition, and drops the topics without a partition
     *
     * @param array<string, list<int>> $partitions
     *
     * @return array<string, list<int>>
     */
    private static function normalized(array $partitions): array
    {
        $normalized = [];
        foreach ($partitions as $topicId => $partitionIds) {
            $partitionIds = array_values(array_unique(array_map(intval(...), $partitionIds)));
            if ($partitionIds === []) {
                continue;
            }
            sort($partitionIds);
            $normalized[(string) $topicId] = $partitionIds;
        }
        ksort($normalized);

        return $normalized;
    }

    /**
     * Returns the partitions of the left side the right one does not hold
     *
     * @param array<string, list<int>> $left
     * @param array<string, list<int>> $right
     *
     * @return array<string, list<int>>
     */
    private static function difference(array $left, array $right): array
    {
        $difference = [];
        foreach ($left as $topicId => $partitionIds) {
            $missing = array_values(array_diff($partitionIds, $right[$topicId] ?? []));
            if ($missing !== []) {
                $difference[(string) $topicId] = $missing;
            }
        }

        return $difference;
    }
}
