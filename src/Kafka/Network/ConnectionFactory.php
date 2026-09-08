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

namespace Protocol\Kafka\Network;

use Closure;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;

/**
 * Opens the connections of the client and keeps them alive between requests.
 *
 * A Kafka connection is a long-lived, ordered request/response channel: the client sends a request and reads the
 * answers of one broker in the very same order, which is exactly why every response carries the correlation id of
 * its request. Re-opening a socket per request would throw that away and pay a TCP handshake each time, so the
 * connection to a broker is cached and handed out again for the next request.
 *
 * A cached connection that has not been used for `connections.max.idle.ms` is closed before it is handed out again:
 * the broker closes idle connections on its own side after that period, and a half-closed socket would only be
 * discovered in the middle of the next request.
 *
 * @see \Protocol\Kafka\Common\Node::getConnection() for the per-broker connections
 * @see \Protocol\Kafka\Common\Cluster::reload() for the bootstrap connections, which are never cached
 */
final class ConnectionFactory
{
    /**
     * Open connections, indexed by their `tcp://host:port` address
     *
     * @var array<string, Stream>
     */
    private static array $connections = [];

    /**
     * Timestamp of the last hand-out of each cached connection, indexed by the address
     *
     * @var array<string, float>
     */
    private static array $lastUsedAt = [];

    /**
     * Factory that creates the underlying stream, only replaced by the tests
     *
     * @var null|Closure(string, array<string, mixed>): Stream
     */
    private static ?Closure $streamFactory = null;

    /**
     * Returns the connection to the given broker, opening it on the first call
     *
     * @param string               $host          Host name of the broker
     * @param int                  $port          Port the broker accepts requests on
     * @param array<string, mixed> $configuration Client configuration
     */
    public static function connect(string $host, int $port, array $configuration = []): Stream
    {
        $address = "tcp://{$host}:{$port}";
        $now     = microtime(true);

        if (isset(self::$connections[$address]) && self::isIdleTooLong($address, $configuration, $now)) {
            self::close($address);
        }
        if (!isset(self::$connections[$address])) {
            self::$connections[$address] = self::open($address, $configuration);
        }
        self::$lastUsedAt[$address] = $now;

        return self::$connections[$address];
    }

    /**
     * Opens a new connection to the given address, without caching it
     *
     * @param string               $address       Address of the broker, e.g. `tcp://127.0.0.1:9092`
     * @param array<string, mixed> $configuration Client configuration
     */
    public static function open(string $address, array $configuration = []): Stream
    {
        $factory = self::$streamFactory;

        return $factory !== null ? $factory($address, $configuration) : new SocketStream($address, $configuration);
    }

    /**
     * Closes and forgets the cached connection to the given address
     */
    public static function close(string $address): void
    {
        $connection = self::$connections[$address] ?? null;
        if ($connection instanceof SocketStream) {
            $connection->disconnect();
        }
        unset(self::$connections[$address], self::$lastUsedAt[$address]);
    }

    /**
     * Closes and forgets the given connection, whatever address it was cached under.
     *
     * The client calls this for a connection whose answer could not be matched to its request any more: the stream
     * position of such a connection is unknown, so it must not be used for another request.
     */
    public static function closeStream(Stream $connection): void
    {
        $address = array_search($connection, self::$connections, true);
        if ($address !== false) {
            self::close($address);
        }
    }

    /**
     * Closes every cached connection
     */
    public static function closeAll(): void
    {
        foreach (array_keys(self::$connections) as $address) {
            self::close($address);
        }
    }

    /**
     * Replaces the factory that creates the underlying streams and drops every cached connection.
     *
     * This is the seam the unit tests use to answer with a scripted response instead of talking to a broker; `null`
     * restores the default {@see SocketStream} factory.
     *
     * @param null|Closure(string, array<string, mixed>): Stream $factory
     */
    public static function useStreamFactory(?Closure $factory): void
    {
        self::closeAll();
        self::$streamFactory = $factory;
    }

    /**
     * Checks whether the cached connection outlived the `connections.max.idle.ms` of the configuration
     *
     * @param array<string, mixed> $configuration Client configuration
     */
    private static function isIdleTooLong(string $address, array $configuration, float $now): bool
    {
        $maxIdleMs = (int) ($configuration[ClientConfig::CONNECTIONS_MAX_IDLE_MS] ?? 0);
        if ($maxIdleMs <= 0) {
            return false;
        }

        return ($now - (self::$lastUsedAt[$address] ?? $now)) * 1000 >= $maxIdleMs;
    }
}
