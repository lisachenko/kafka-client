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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Throwable;

/**
 * Keeps the **incremental fetch session** (KIP-227, Kafka 1.1) that a consumer holds with one broker.
 *
 * This is the `org.apache.kafka.clients.FetchSessionHandler` of the Java client @ 1.1.1, and it does what that one
 * does: there is one handler per broker node, it remembers which partitions the session on that node holds and at
 * which fetch offset each of them stands, it decides what the next request has to state and what it may leave out,
 * and it moves the {@see FetchMetadata} of the session on with every answer. The counterpart on the broker is
 * `kafka.server.FetchManager` (`core/.../FetchSession.scala`).
 *
 * One round looks like this:
 *
 * ```php
 * $builder = $handler->newBuilder();
 * foreach ($positions as $topic => $partitionOffsets) {
 *     foreach ($partitionOffsets as $partition => $offset) {
 *         $builder->add(new TopicPartition($topic, $partition), $offset);
 *     }
 * }
 * $data    = $builder->build();                       // what to send, what to forget, with which metadata
 * $request = new FetchRequest($data->toSend, …, $data->metadata, $data->toForget);
 * …
 * if ($handler->handleResponse($response)) {          // advances the epoch, or starts over
 *     // the answer may be used
 * }
 * ```
 *
 * The **first** request of a handler is a full fetch with the epoch 0 ({@see FetchMetadata::initial()}), which asks
 * the broker to open a session; every following one is incremental and states only the partitions whose fetch offset
 * moved since the previous request, plus the partitions that left the set in `forgotten_topics_data`. That is the
 * whole point of KIP-227: a consumer of many partitions repeats neither the list nor its parameters.
 *
 * **What the broker answers, and what this class makes of it** (measured on the 1.1.1 container, see the protocol
 * document):
 *
 * * a full fetch is answered with every partition of the request and with the id of the session the broker opened;
 *   the next request of the handler is then the incremental fetch of that id with the epoch 1;
 * * an incremental fetch is answered with **only** the partitions that have news - a partition whose fetch offset
 *   the client did not move is answered again, because the session keeps that offset, and a partition that has
 *   nothing new is left out entirely, up to an answer with no topic at all. The caller therefore has to keep the
 *   state of every partition the answer does not mention;
 * * the error codes **70** (`FETCH_SESSION_ID_NOT_FOUND`) and **71** (`INVALID_FETCH_SESSION_EPOCH`) arrive as the
 *   *top-level* error code of the answer, with the session id **0** and an empty topics array. Neither destroys the
 *   session on the broker, and neither may be taken as "my session is 0": the handler keeps its own id and answers
 *   a 71 with {@see FetchMetadata::nextCloseExisting()}, the full fetch that closes the old session and opens a new
 *   one in a single request. A 70 means that the id is gone anyway - the broker evicted the session, its cache holds
 *   `max.incremental.fetch.session.cache.slots` (1000) of them - so the handler starts over with
 *   {@see FetchMetadata::initial()}, exactly as the Java client does;
 * * an answer whose session id is 0 although the request had one means that the broker closed the session (the last
 *   partition of it was forgotten, or it was created without one); the handler starts over as well.
 *
 * A request that never reached the broker or whose answer never arrived - a dropped connection - is reported with
 * {@see self::handleError()}, which makes the next request the full fetch of `nextCloseExisting()`. The connection
 * itself is not part of a session: a broker keeps the session of a client that reconnects, but the client can not
 * know how much of the last request the broker processed, so it starts over.
 *
 * @see docs/protocol/2.8.md, section "Fetch sessions (v7, KIP-227)"
 * @see \Protocol\Kafka\Client::fetchPartitionsWithSessions()
 */
final class FetchSessionHandler
{
    /**
     * Metadata the next request of this session travels with
     */
    private FetchMetadata $nextMetadata;

    /**
     * Every partition the session holds, as `topic => partition => fetch offset`, in the order they are asked for
     *
     * @var array<string, array<int, int>>
     */
    private array $sessionPartitions = [];

    /**
     * @param int $node Id of the broker node this session lives on, for the messages of the client
     */
    public function __construct(public readonly int $node = -1)
    {
        $this->nextMetadata = FetchMetadata::initial();
    }

    /**
     * Starts building the next request of this session
     *
     * The partitions are added in the order the request should ask for them; {@see FetchRequestData} is what comes
     * out, and building it moves the state of the session on - a builder is therefore used exactly once.
     */
    public function newBuilder(): FetchSessionHandlerBuilder
    {
        return new FetchSessionHandlerBuilder(fn(array $next): FetchRequestData => $this->buildRequestData($next));
    }

    /**
     * Returns the metadata the next request of this session travels with
     */
    public function getNextMetadata(): FetchMetadata
    {
        return $this->nextMetadata;
    }

    /**
     * Returns the id of the session, {@see FetchMetadata::INVALID_SESSION_ID} while there is none
     */
    public function getSessionId(): int
    {
        return $this->nextMetadata->sessionId;
    }

    /**
     * Returns every partition the session holds, as `topic => partition => fetch offset`
     *
     * @return array<string, array<int, int>>
     */
    public function getSessionPartitions(): array
    {
        return $this->sessionPartitions;
    }

    /**
     * Takes the answer of the request that was built last and tells whether it may be used
     *
     * `false` means that the answer carries nothing to read - a session error, or a set of partitions that does not
     * match the session - and that the handler has put itself back to a full fetch; the caller simply asks again.
     */
    public function handleResponse(FetchResponse $response): bool
    {
        if ($response->errorCode !== KafkaException::NO_ERROR) {
            // 70 says that the id is gone, so there is nothing left to close; every other session error - 71 - is
            // answered with the full fetch that removes whatever the broker still holds and opens a new session
            $this->nextMetadata = $response->errorCode === KafkaException::FETCH_SESSION_ID_NOT_FOUND
                ? FetchMetadata::initial()
                : $this->nextMetadata->nextCloseExisting();

            return false;
        }

        $answered = self::partitionsOf($response);
        if ($this->nextMetadata->isFull()) {
            // A full fetch is answered with the whole set it asked for, no more and no less
            if (self::findMissing($answered, $this->sessionPartitions) !== []
                || self::findMissing($this->sessionPartitions, $answered) !== []) {
                $this->nextMetadata = FetchMetadata::initial();

                return false;
            }
            $this->nextMetadata = $response->sessionId === FetchMetadata::INVALID_SESSION_ID
                ? FetchMetadata::initial()
                : FetchMetadata::newIncremental($response->sessionId);

            return true;
        }

        // An incremental fetch may be answered with any subset of the session, but never with a partition outside it
        if (self::findMissing($answered, $this->sessionPartitions) !== []) {
            $this->nextMetadata = $this->nextMetadata->nextCloseExisting();

            return false;
        }
        $this->nextMetadata = $response->sessionId === FetchMetadata::INVALID_SESSION_ID
            ? FetchMetadata::initial()
            : $this->nextMetadata->nextIncremental();

        return true;
    }

    /**
     * Reports that the request could not be sent or that its answer never arrived
     *
     * The next request is then the full fetch of {@see FetchMetadata::nextCloseExisting()}: whatever the broker did
     * with the request that was lost, the session it may still hold is closed and a new one is opened in its place.
     *
     * @param Throwable|null $error Failure that was observed, for a caller that logs it
     */
    public function handleError(?Throwable $error = null): void
    {
        unset($error);

        $this->nextMetadata = $this->nextMetadata->nextCloseExisting();
    }

    /**
     * Works out what the next request has to carry and moves the session on
     *
     * A **full** fetch replaces the whole set: everything that was added is sent, nothing is forgotten - the broker
     * throws the old set away anyway. An **incremental** one sends the partitions whose fetch offset differs from
     * the one the session holds plus the partitions that were not in it yet, forgets the ones that are gone from
     * the wanted set, and leaves everything else out.
     *
     * @param array<string, array<int, int>> $next Partitions the caller wants to read, as topic => partition =>
     *                                             fetch offset, in the order they should be asked for
     */
    private function buildRequestData(array $next): FetchRequestData
    {
        if ($this->nextMetadata->isFull()) {
            $this->sessionPartitions = $next;

            return new FetchRequestData($next, [], $next, $this->nextMetadata);
        }

        $toSend   = [];
        $toForget = [];
        foreach ($this->sessionPartitions as $topic => $partitions) {
            foreach ($partitions as $partition => $sessionOffset) {
                if (!isset($next[$topic][$partition])) {
                    // The partition left the wanted set - a rebalance, a pause, a topic that is gone
                    unset($this->sessionPartitions[$topic][$partition]);
                    $toForget[$topic][] = $partition;
                    continue;
                }
                if ($next[$topic][$partition] === $sessionOffset) {
                    // The broker knows this offset already, the request does not have to repeat it
                    continue;
                }
                // The altered partition moves behind the other partitions of its topic, like the Java builder moves
                // it behind the whole set: the answer is filled in the order of the request
                unset($this->sessionPartitions[$topic][$partition]);
                $this->sessionPartitions[$topic][$partition] = $next[$topic][$partition];
                $toSend[$topic][$partition]                  = $next[$topic][$partition];
            }
            if ($this->sessionPartitions[$topic] === []) {
                unset($this->sessionPartitions[$topic]);
            }
        }

        foreach ($next as $topic => $partitions) {
            foreach ($partitions as $partition => $fetchOffset) {
                if (isset($this->sessionPartitions[$topic][$partition])) {
                    continue;
                }
                $this->sessionPartitions[$topic][$partition] = $fetchOffset;
                $toSend[$topic][$partition]                  = $fetchOffset;
            }
        }

        return new FetchRequestData($toSend, $toForget, $this->sessionPartitions, $this->nextMetadata);
    }

    /**
     * Returns the partitions of an answer, as topic => partition => true
     *
     * @return array<string, array<int, bool>>
     */
    private static function partitionsOf(FetchResponse $response): array
    {
        $partitions = [];
        foreach ($response->topics as $topic => $topicResponse) {
            foreach (array_keys($topicResponse->partitions) as $partition) {
                $partitions[(string) $topic][(int) $partition] = true;
            }
        }

        return $partitions;
    }

    /**
     * Returns the partitions of the first set that the second one does not hold, as `topic-partition` names
     *
     * This is the `findMissing()` of the Java client, which the two verifications of an answer are built on.
     *
     * @param array<string, array<int, mixed>> $toFind   Partitions to look for
     * @param array<string, array<int, mixed>> $toSearch Partitions to look in
     *
     * @return list<string>
     */
    private static function findMissing(array $toFind, array $toSearch): array
    {
        $missing = [];
        foreach ($toFind as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                if (!isset($toSearch[$topic][$partition])) {
                    $missing[] = (string) new TopicPartition((string) $topic, (int) $partition);
                }
            }
        }

        return $missing;
    }
}
