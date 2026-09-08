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

use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;

/**
 * Replaces the connections of {@see ConnectionFactory} with scripted ones, so that a cluster and a client can be
 * driven without a broker.
 *
 * Each address is given the answers of the connection that will be opened to it; an address that is not scripted
 * behaves like a broker that does not accept connections. An address may also be passed through to a real socket,
 * which is what the tests of the timeout behaviour need.
 */
final class ScriptedConnections
{
    /**
     * Connections that are still to be handed out for an address, in order
     *
     * @var array<string, list<BrokerConnection>>
     */
    private array $scripts = [];

    /**
     * Addresses that are connected with a real socket instead of a double
     *
     * @var list<string>
     */
    private array $realAddresses = [];

    /**
     * Every address a connection was opened to, in order
     *
     * @var list<string>
     */
    private array $openedAddresses = [];

    /**
     * Connections that were handed out for an address
     *
     * @var array<string, list<BrokerConnection>>
     */
    private array $handedOut = [];

    /**
     * Scripts the connections that will be opened to the given address, one entry per connection
     */
    public function on(string $address, BrokerConnection ...$connections): self
    {
        foreach ($connections as $connection) {
            $this->scripts[$address][] = $connection;
        }

        return $this;
    }

    /**
     * Lets the connections to the given address use a real socket
     */
    public function passThrough(string $address): self
    {
        $this->realAddresses[] = $address;

        return $this;
    }

    /**
     * Installs this script as the connection factory of the client
     */
    public function install(): void
    {
        ConnectionFactory::useStreamFactory(
            function (string $address, array $configuration): Stream {
                $this->openedAddresses[] = $address;

                if (in_array($address, $this->realAddresses, true)) {
                    return new SocketStream($address, $configuration, 1.0);
                }

                $connection = isset($this->scripts[$address]) ? array_shift($this->scripts[$address]) : null;
                if ($connection === null) {
                    throw new NetworkException(
                        ['error' => 'Connection refused by the scripted broker', 'address' => $address]
                    );
                }
                $this->handedOut[$address][] = $connection;

                return $connection;
            }
        );
    }

    /**
     * Restores the real connection factory
     */
    public static function uninstall(): void
    {
        ConnectionFactory::useStreamFactory(null);
    }

    /**
     * Returns every address a connection was opened to, in order
     *
     * @return list<string>
     */
    public function getOpenedAddresses(): array
    {
        return $this->openedAddresses;
    }

    /**
     * Returns how many connections were opened to the given address
     */
    public function getConnectionCount(string $address): int
    {
        return count(array_filter($this->openedAddresses, static fn(string $opened): bool => $opened === $address));
    }

    /**
     * Returns the connections that were handed out for the given address
     *
     * @return list<BrokerConnection>
     */
    public function getConnections(string $address): array
    {
        return $this->handedOut[$address] ?? [];
    }
}
