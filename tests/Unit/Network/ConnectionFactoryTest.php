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

namespace Protocol\Kafka\Tests\Unit\Network;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;

/**
 * Tests the lifetime of the connections that the client keeps to the brokers
 */
#[CoversClass(ConnectionFactory::class)]
#[CoversClass(Node::class)]
final class ConnectionFactoryTest extends TestCase
{
    /**
     * Number of streams the test factory created
     */
    private int $createdStreams = 0;

    protected function setUp(): void
    {
        $this->createdStreams = 0;
        ConnectionFactory::useStreamFactory(function (): Stream {
            $this->createdStreams++;

            return new BrokerConnection();
        });
    }

    protected function tearDown(): void
    {
        ConnectionFactory::useStreamFactory(null);
    }

    public function testTheConnectionToOneBrokerIsOpenedOnceAndReused(): void
    {
        $first  = ConnectionFactory::connect('broker-1', 9092);
        $second = ConnectionFactory::connect('broker-1', 9092);

        self::assertSame($first, $second, 'a Kafka connection is a long-lived request/response channel');
        self::assertSame(1, $this->createdStreams);
    }

    public function testEveryBrokerGetsItsOwnConnection(): void
    {
        $first  = ConnectionFactory::connect('broker-1', 9092);
        $second = ConnectionFactory::connect('broker-2', 9092);
        $third  = ConnectionFactory::connect('broker-1', 9093);

        self::assertNotSame($first, $second);
        self::assertNotSame($first, $third, 'the port is part of the identity of a broker');
        self::assertSame(3, $this->createdStreams);
    }

    public function testANodeHandsOutTheConnectionOfItsOwnAddress(): void
    {
        $node = Node::__set_state(['nodeId' => 7, 'host' => 'broker-1', 'port' => 9092]);

        self::assertSame(ConnectionFactory::connect('broker-1', 9092), $node->getConnection([]));
        self::assertSame(1, $this->createdStreams);
    }

    public function testAnUnusedConnectionIsReopenedAfterTheMaximumIdleTime(): void
    {
        $configuration = [ClientConfig::CONNECTIONS_MAX_IDLE_MS => 20];

        $first = ConnectionFactory::connect('broker-1', 9092, $configuration);
        usleep(40000);
        $second = ConnectionFactory::connect('broker-1', 9092, $configuration);

        self::assertNotSame($first, $second, 'the broker drops an idle connection on its side as well');
        self::assertSame(2, $this->createdStreams);
    }

    public function testAConnectionThatIsUsedInTimeStaysOpen(): void
    {
        $configuration = [ClientConfig::CONNECTIONS_MAX_IDLE_MS => 10000];

        $first  = ConnectionFactory::connect('broker-1', 9092, $configuration);
        $second = ConnectionFactory::connect('broker-1', 9092, $configuration);

        self::assertSame($first, $second);
        self::assertSame(1, $this->createdStreams);
    }

    public function testIdleConnectionsAreKeptForeverWhenTheOptionIsNotConfigured(): void
    {
        $first = ConnectionFactory::connect('broker-1', 9092);
        usleep(5000);
        $second = ConnectionFactory::connect('broker-1', 9092);

        self::assertSame($first, $second);
    }

    public function testADesynchronizedConnectionIsDroppedAndOpenedAgain(): void
    {
        $connection = ConnectionFactory::connect('broker-1', 9092);

        ConnectionFactory::closeStream($connection);

        self::assertNotSame($connection, ConnectionFactory::connect('broker-1', 9092));
        self::assertSame(2, $this->createdStreams);
    }

    public function testClosingAnUnknownConnectionIsHarmless(): void
    {
        ConnectionFactory::closeStream(new BrokerConnection());
        ConnectionFactory::close('tcp://not-connected:9092');

        self::assertSame(0, $this->createdStreams);
    }

    public function testEveryConnectionIsClosedAtOnce(): void
    {
        $first  = ConnectionFactory::connect('broker-1', 9092);
        $second = ConnectionFactory::connect('broker-2', 9092);

        ConnectionFactory::closeAll();

        self::assertNotSame($first, ConnectionFactory::connect('broker-1', 9092));
        self::assertNotSame($second, ConnectionFactory::connect('broker-2', 9092));
    }

    public function testTheBootstrapConnectionIsNeverCached(): void
    {
        $first  = ConnectionFactory::open('tcp://broker-1:9092');
        $second = ConnectionFactory::open('tcp://broker-1:9092');

        self::assertNotSame($first, $second, 'the metadata request opens its own connection every time');
        self::assertSame(2, $this->createdStreams);
    }

    public function testTheDefaultFactoryBuildsPlainSocketStreams(): void
    {
        ConnectionFactory::useStreamFactory(null);

        // Without `security.protocol = SSL` a connection is a plain TCP socket, as on every broker before Kafka 0.9
        self::assertInstanceOf(SocketStream::class, ConnectionFactory::open('tcp://127.0.0.1:9092'));
    }
}
