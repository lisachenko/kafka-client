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
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Tests\Fixture\ClusterMetadataResponse;

/**
 * Verifies the request/response framing against a real Kafka 0.10.2.2 broker.
 *
 * A Metadata request is the cheapest round trip that any broker of the cluster answers. The framing itself has not
 * changed since 0.8.2.2 - a size-prefixed request, a size-prefixed response, correlation ids echoed back in order;
 * which api keys and versions the broker frames an answer for at all is the subject of {@see ApiVersionProbeTest}.
 *
 * What Kafka 0.10 did change is the fate of a frame the broker cannot parse. A 0.9.0.1 broker dropped it and kept
 * the connection open, so the framing of everything that followed on that connection was unaffected. A 0.10.2.2
 * broker **closes the connection** instead (`SocketServer.processCompletedReceives` @ 0.10.2.2), which a client sees
 * as the end of the stream while it waits for the response - the last test below pins that, because it is the
 * difference between "read the next answer" and "reconnect" for every caller of this client.
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

    /**
     * A frame the broker cannot parse ends the connection, and the client sees it as a dropped stream
     *
     * The frame is a Metadata request with the version 5, which Kafka 1.0 has and 0.11.0.3 does not - a 0.11.0.3
     * broker serves Metadata up to v4. It is built by hand, because the point is to send something the request
     * classes of this branch deliberately cannot build. `RequestChannel.Request` @ 0.11.0.3 throws an
     * `InvalidRequestException` for it and `SocketServer.processCompletedReceives` closes the channel, so the read
     * of the response runs into the end of the stream instead of into a timeout - the 0.9.0.1 behaviour this
     * replaces.
     */
    public function testAFrameTheBrokerCannotParseClosesTheConnection(): void
    {
        $body   = pack('n', 3) . pack('n', 5) . pack('N', 4243) . pack('n', 0) . pack('N', -1);
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => 5000]);
        $stream->write('N', strlen($body));
        $stream->writeBuffer($body);

        $this->expectException(NetworkException::class);

        ClusterMetadataResponse::unpack($stream);
    }

    /**
     * The connection that was closed is the only casualty: a new one answers immediately afterwards
     */
    public function testTheNextConnectionIsAnsweredAfterTheBrokerClosedOne(): void
    {
        $body   = pack('n', 3) . pack('n', 5) . pack('N', 4244) . pack('n', 0) . pack('N', -1);
        $broken = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => 5000]);
        $broken->write('N', strlen($body));
        $broken->writeBuffer($body);

        try {
            ClusterMetadataResponse::unpack($broken);
            self::fail('The broker has to close the connection for a Metadata v5 frame');
        } catch (NetworkException) {
            // expected: the broker closed the socket
        }

        $stream = $this->connect();
        new MetadataRequest([], 'kafka-client-t1', 4245)->writeTo($stream);

        self::assertSame(4245, ClusterMetadataResponse::unpack($stream)->getCorrelationId());
    }
}
