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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Tests\Fixture\ClusterMetadataResponse;

/**
 * Verifies the request/response framing against a real Kafka 0.8.2.2 broker.
 *
 * A Metadata request is the cheapest round trip that any broker of the cluster answers.
 */
#[CoversClass(AbstractProtocolMessage::class)]
#[CoversClass(AbstractRequest::class)]
#[CoversClass(AbstractResponse::class)]
#[CoversClass(BinarySchema::class)]
#[CoversClass(SocketStream::class)]
final class ProtocolFramingTest extends IntegrationTestCase
{
    public function testBrokerAnswersAMetadataRequestWithTheSameCorrelationId(): void
    {
        $stream = $this->connect();
        new MetadataRequest([], 'kafka-client-t1', 4242)->writeTo($stream);

        self::assertSame(4242, ClusterMetadataResponse::unpack($stream)->getCorrelationId());
    }

    public function testEveryResponseCarriesTheCorrelationIdOfItsOwnRequest(): void
    {
        $stream = $this->connect();

        foreach ([1, 2, 0, -1, 2147483647] as $correlationId) {
            new MetadataRequest([], 'kafka-client-t1', $correlationId)->writeTo($stream);

            self::assertSame($correlationId, ClusterMetadataResponse::unpack($stream)->getCorrelationId());
        }
    }

    public function testAdvertisedBrokerMatchesTheBootstrapAddress(): void
    {
        $addresses = array_map(
            static fn($broker): string => $broker->host . ':' . $broker->port,
            self::clusterBrokers()
        );

        self::assertNotEmpty($addresses, 'The readiness probe must have resolved the cluster before the tests run');
        self::assertContains(self::firstBootstrapServer(), $addresses);
    }

    public function testBrokerListIsDecodedByTheSchemaEngine(): void
    {
        $stream = $this->connect();
        new MetadataRequest([], 'kafka-client-t1', 1)->writeTo($stream);

        $response = ClusterMetadataResponse::unpack($stream);

        self::assertSame(1, $response->getCorrelationId());
        self::assertNotEmpty($response->brokers);
        foreach ($response->brokers as $nodeId => $broker) {
            self::assertSame($nodeId, $broker->nodeId, 'the array has to be indexed by the node id of its items');
            self::assertNotSame('', $broker->host);
            self::assertGreaterThan(0, $broker->port);
        }
    }

    public function testMetadataOfAnAutoCreatedTopicRoundTrips(): void
    {
        $topic  = self::uniqueTopicName('t1-framing');
        $stream = $this->connect();
        new MetadataRequest([$topic], 'kafka-client-t1', 99)->writeTo($stream);

        $response = ClusterMetadataResponse::unpack($stream);

        self::assertSame(99, $response->getCorrelationId());
        self::assertNotEmpty($response->brokers);
    }
}
