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

namespace Protocol\Kafka\Protocol\Request;

/**
 * The `session_id` and the `epoch` a Fetch request of version 7 travels with (KIP-227, Kafka 1.1)
 *
 * This is `org.apache.kafka.common.requests.FetchMetadata` of the Java client, field for field. The two int32 that
 * version 7 of the Fetch api added in front of the topics array are a tiny state machine, and every state of it has
 * a name here - the two that the Java client keeps as the constants `LEGACY` and `INITIAL` are static factory
 * methods here, because a PHP class constant can not hold an object:
 *
 * | Metadata | session id | epoch | What the broker does |
 * |---|---:|---:|---|
 * | {@see self::legacy()} (`FetchMetadata.LEGACY`) | 0 | -1 | A session-less full fetch, the request every version below 7 sends; the answer is the whole requested set with `session_id = 0` |
 * | {@see self::initial()} (`FetchMetadata.INITIAL`) | 0 | 0 | A full fetch that **creates** a session; the answer carries the id of the new session |
 * | {@see self::newIncremental()} | the id | 1 | The first incremental fetch of that session |
 * | {@see self::nextIncremental()} | the id | n+1 | Every following incremental fetch |
 * | {@see self::nextCloseExisting()} | the id | 0 | A full fetch that closes the session and asks for a new one |
 * | session id with the epoch -1 | the id | -1 | Closes the session and answers session-less |
 *
 * An **incremental** fetch (any epoch above 0) sends only the partitions whose fetch parameters changed and is
 * answered with only the partitions that have news; a **full** fetch (`isFull()`, i.e. the epoch 0 or -1) sends the
 * whole set and is answered with all of it. The partitions a session should forget travel in the
 * `forgotten_topics_data` array of the request, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopic}.
 *
 * The value object is immutable: every transition returns a new instance, as it does in the Java client.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v10)" and "Fetch sessions (v7, KIP-227)"
 */
final class FetchMetadata
{
    /**
     * The session ID used by clients with no session
     */
    public const int INVALID_SESSION_ID = 0;

    /**
     * The first epoch: in a fetch request it says that the client wants to create or recreate a session
     */
    public const int INITIAL_EPOCH = 0;

    /**
     * An invalid epoch: in a fetch request it says that the client wants to close any existing session without
     * creating a new one
     */
    public const int FINAL_EPOCH = -1;

    /**
     * The highest epoch the wire has room for, the `Integer.MAX_VALUE` of the Java client
     */
    private const int MAX_EPOCH = 2147483647;

    /**
     * Returns the metadata that a request without fetch sessions travels with, i.e. a session-less full fetch
     *
     * This is what every version below 7 sends implicitly and what {@see FetchRequest} sends when it is given no
     * metadata at all; a 1.1.1 broker serves it exactly as it serves a Fetch v6 and answers `session_id = 0`.
     */
    public static function legacy(): self
    {
        return new self(self::INVALID_SESSION_ID, self::FINAL_EPOCH);
    }

    /**
     * Returns the metadata of the very first request of a new session: a full fetch that asks for a session id
     */
    public static function initial(): self
    {
        return new self(self::INVALID_SESSION_ID, self::INITIAL_EPOCH);
    }

    /**
     * Returns the metadata of the first incremental request of the session the broker just handed out
     */
    public static function newIncremental(int $sessionId): self
    {
        return new self($sessionId, self::nextEpoch(self::INITIAL_EPOCH));
    }

    /**
     * Returns the epoch that follows the given one
     *
     * The successor of {@see self::FINAL_EPOCH} is `FINAL_EPOCH` itself, and the successor of `PHP_INT_MAX` - the
     * `Integer.MAX_VALUE` of the Java client, which this client can only reach through a hand-built request - is 1,
     * because the epoch 0 means "full fetch".
     */
    public static function nextEpoch(int $previousEpoch): int
    {
        return match (true) {
            $previousEpoch < 0                => self::FINAL_EPOCH,
            $previousEpoch === self::MAX_EPOCH => 1,
            default                           => $previousEpoch + 1,
        };
    }

    public function __construct(
        /**
         * Id of the fetch session, {@see self::INVALID_SESSION_ID} for a request that has none
         */
        public readonly int $sessionId,
        /**
         * Epoch of the fetch session, {@see self::INITIAL_EPOCH} for a full fetch that creates one and
         * {@see self::FINAL_EPOCH} for a request that closes one or has no session at all
         */
        public readonly int $epoch
    ) {}

    /**
     * Tells whether this is a full fetch request, i.e. one that carries every partition the client wants to read
     */
    public function isFull(): bool
    {
        return $this->epoch === self::INITIAL_EPOCH || $this->epoch === self::FINAL_EPOCH;
    }

    /**
     * Returns the metadata of the next incremental request of this session
     */
    public function nextIncremental(): self
    {
        return new self($this->sessionId, self::nextEpoch($this->epoch));
    }

    /**
     * Returns the metadata of a full fetch that closes this session and asks for a new one
     *
     * `FetchSessionCache.newContext` @ 1.1.1 removes the session that a full fetch names before it creates the new
     * one, so this is how a client starts over after the error codes 70 and 71 without leaking the old session.
     */
    public function nextCloseExisting(): self
    {
        return new self($this->sessionId, self::INITIAL_EPOCH);
    }

    /**
     * Returns the representation the Java client logs, e.g. `(sessionId=INVALID, epoch=FINAL)`
     */
    public function __toString(): string
    {
        $sessionId = $this->sessionId === self::INVALID_SESSION_ID ? 'INVALID' : (string) $this->sessionId;
        $epoch     = match ($this->epoch) {
            self::INITIAL_EPOCH => 'INITIAL',
            self::FINAL_EPOCH   => 'FINAL',
            default             => (string) $this->epoch,
        };

        return "(sessionId={$sessionId}, epoch={$epoch})";
    }
}
