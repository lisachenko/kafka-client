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
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV2;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV2;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Tests\Fixture\ClusterMetadataResponse;

/**
 * Verifies the request/response framing against a real Kafka 2.8.2 broker.
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
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(BinarySchema::class)]
#[CoversClass(SocketStream::class)]
final class ProtocolFramingTest extends IntegrationTestCase
{
    public function testBrokerAnswersAMetadataRequestWithTheSameCorrelationId(): void
    {
        $stream = $this->connect();
        new MetadataRequest([], true, 'kafka-client-t1', 4242)->writeTo($stream);

        self::assertSame(4242, ClusterMetadataResponse::unpack($stream)->getCorrelationId());
    }

    public function testEveryResponseCarriesTheCorrelationIdOfItsOwnRequest(): void
    {
        $stream = $this->connect();

        foreach ([1, 2, 0, -1, 2147483647] as $correlationId) {
            new MetadataRequest([], true, 'kafka-client-t1', $correlationId)->writeTo($stream);

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
        new MetadataRequest([], true, 'kafka-client-t1', 1)->writeTo($stream);

        // The reduced ClusterMetadataResponse fixture reads the version 0 layout; a version 4 answer opens with
        // the throttle time of KIP-124, so it has to be decoded with the real class
        $response = MetadataResponse::unpack($stream);

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
        new MetadataRequest([$topic], true, 'kafka-client-t1', 99)->writeTo($stream);

        $response = MetadataResponse::unpack($stream);

        self::assertSame(99, $response->getCorrelationId());
        self::assertNotEmpty($response->brokers);
    }

    /**
     * The flexible encoding of KIP-482 end to end, through the schema engine and a real socket
     *
     * ApiVersions v3 is the first flexible frame this client sends, and it exercises every piece of the encoding at
     * once: a **request header v2** whose `client_id` stays plain and whose tag buffer is empty, two **compact**
     * strings of KIP-511 in the body, a body tag buffer of its own - and, coming back, a **compact array** of 56
     * entries that each end in a tag buffer, a tagged field (the finalized-features epoch of KIP-584) and the one
     * response header the flexible versions did *not* change, because this api keeps the header v0.
     *
     * The two encodings live on the same connection: the v2 request that follows is written plainly on the socket
     * the v3 request was written compactly on, and the broker answers both.
     */
    public function testAFlexibleFrameIsWrittenAndReadByTheSchemaEngine(): void
    {
        $stream  = $this->connect();
        $request = new ApiVersionsRequest('kafka-client-t1', 4246);
        $request->writeTo($stream);

        $response = ApiVersionsResponse::unpack($stream);

        self::assertTrue(ApiVersionsRequest::isFlexible(), 'the client sends the flexible v3');
        self::assertSame(4246, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode, 'the broker accepted the compact body and the two KIP-511 strings');
        self::assertCount(56, $response->apiVersions, 'the compact array of the api table');
        self::assertSame(3, $response->maxVersionOf(18));
        self::assertSame(
            0,
            $response->finalizedFeaturesEpoch,
            'the one tagged field a ZooKeeper-backed 2.8.2 broker answers (KIP-584)'
        );
        self::assertSame([], $response->supportedFeatures);
        self::assertSame([], $response->finalizedFeatures);

        // The plain encoding still works, on the very same connection
        new ApiVersionsRequestV2('kafka-client-t1', 4247)->writeTo($stream);
        $versionTwo = ApiVersionsResponseV2::unpack($stream);

        self::assertSame(4247, $versionTwo->getCorrelationId());
        self::assertSame(array_keys($response->apiVersions), array_keys($versionTwo->apiVersions));
    }

    /**
     * The bytes of a flexible request are the bytes the broker accepted, and it re-encodes to itself
     */
    public function testTheFlexibleRequestIsTheFrameTheBrokerAccepted(): void
    {
        $request = new ApiVersionsRequest('kafka-client-t1', 4248);
        $frame   = (string) $request;

        // 4 size + 8 header + 2+15 client id + 1 header tag buffer + 24 name + 4 version + 1 body tag buffer
        self::assertSame(59, strlen($frame));
        self::assertSame('0012' . '0003', bin2hex(substr($frame, 4, 4)));
        self::assertSame('000f', bin2hex(substr($frame, 12, 2)), 'the client id keeps its int16 length');
        self::assertSame('00', bin2hex(substr($frame, 29, 1)), 'the tag buffer of the request header v2');
        self::assertSame('18', bin2hex(substr($frame, 30, 1)), 'a compact string of 23 bytes announces 24');
        self::assertSame('00', bin2hex(substr($frame, -1)), 'and the body ends in its own tag buffer');

        $stream = $this->connect();
        $stream->writeBuffer($frame);

        self::assertSame(4248, ApiVersionsResponse::unpack($stream)->getCorrelationId());
    }

    /**
     * A frame the broker cannot parse ends the connection, and the client sees it as a dropped stream
     *
     * The frame is a Metadata request of the version 5 whose body stops after the topic array: a 2.8.2 broker
     * serves that version, but `allow_auto_topic_creation` is missing, so the parser runs past the end of the
     * buffer. It is built by hand, because the point is to send something the request classes of this branch
     * deliberately cannot build. `RequestContext.parseRequest` @ 2.8.2 throws an `InvalidRequestException` for it
     * and `SocketServer.processCompletedReceives` closes the channel, so the read of the response runs into the end
     * of the stream instead of into a timeout - the 0.9.0.1 behaviour this replaces.
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
        new MetadataRequest([], true, 'kafka-client-t1', 4245)->writeTo($stream);

        self::assertSame(4245, ClusterMetadataResponse::unpack($stream)->getCorrelationId());
    }
}
