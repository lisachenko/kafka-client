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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Tests\Integration\Fixture\BrokerListResponse;

/**
 * Verifies the request/response framing against a real Kafka 0.8.2.2 broker.
 *
 * A Metadata request with an empty topic list is the cheapest round trip that any broker in the cluster answers.
 */
#[CoversClass(AbstractProtocolMessage::class)]
#[CoversClass(AbstractRequest::class)]
#[CoversClass(AbstractResponse::class)]
#[CoversClass(SocketStream::class)]
final class ProtocolFramingTest extends IntegrationTestCase
{
    public function testBrokerEchoesTheCorrelationIdOfAMetadataRequest(): void
    {
        $stream  = $this->connect();
        $request = new MetadataRequest([], 'kafka-client-t1', 4242);
        $request->writeTo($stream);

        $response = BrokerListResponse::unpackFrom($stream);

        self::assertSame(4242, $response->getCorrelationId());
        self::assertNotEmpty($response->brokers, 'The broker must advertise at least itself');

        foreach ($response->brokers as $broker) {
            self::assertNotSame('', $broker['host']);
            self::assertGreaterThan(0, $broker['port']);
        }
    }

    public function testEveryResponseCarriesTheCorrelationIdOfItsOwnRequest(): void
    {
        $stream = $this->connect();

        foreach ([1, 2, 0, -1, 2147483647] as $correlationId) {
            new MetadataRequest([], 'kafka-client-t1', $correlationId)->writeTo($stream);

            self::assertSame($correlationId, BrokerListResponse::unpackFrom($stream)->getCorrelationId());
        }
    }

    public function testAdvertisedBrokerMatchesTheBootstrapAddress(): void
    {
        $stream = $this->connect();
        new MetadataRequest([], 'kafka-client-t1', 1)->writeTo($stream);

        $response  = BrokerListResponse::unpackFrom($stream);
        $addresses = array_map(
            static fn(array $broker): string => $broker['host'] . ':' . $broker['port'],
            $response->brokers
        );

        self::assertContains(self::firstBootstrapServer(), $addresses);
    }
}
