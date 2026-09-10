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

use Protocol\Kafka\Client;
use Protocol\Kafka\Consumer\Internals\FetchSessionHandler;
use Protocol\Kafka\Consumer\KafkaConsumer;

/**
 * A consumer that lets a test look at the incremental fetch sessions it holds.
 *
 * The sessions of KIP-227 are invisible to an application on purpose - a `poll()` returns records whether the
 * request was a full fetch or an incremental one, and the recovery from a session error happens inside the client -
 * so the only way to assert *which* request went out is to ask the client for its session state, which is what the
 * one method of this class is for. Everything else is the consumer of the package.
 *
 * @see \Protocol\Kafka\Tests\Integration\FetchSessionConsumerTest
 */
final class SessionAwareConsumer extends KafkaConsumer
{
    /**
     * Returns the fetch session this consumer holds with every broker it read from, by node id
     *
     * @return array<int, FetchSessionHandler>
     */
    public function fetchSessions(): array
    {
        return $this->getClient()->getFetchSessionHandlers();
    }

    /**
     * Returns the fetch session of one broker, or `null` when this consumer has not read from it yet
     */
    public function fetchSession(int $nodeId): ?FetchSessionHandler
    {
        return $this->fetchSessions()[$nodeId] ?? null;
    }

    /**
     * Returns the low-level client of this consumer
     */
    public function client(): Client
    {
        return $this->getClient();
    }
}
