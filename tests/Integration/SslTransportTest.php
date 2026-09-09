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
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Common\Security\SslProtocol;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\SpecMessageSet;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the SSL transport against the SSL listener of a real Kafka 0.9.0.1 broker.
 *
 * Kafka 0.9.0.0 is the release that gave a broker one listener per security protocol. The requests themselves are
 * untouched by that - the bytes on an encrypted connection are the bytes of a plain one - so what is tested here is
 * the transport: that the handshake succeeds against the certificate of the broker, that it fails against any other
 * trust anchor, and above all *which endpoint the broker advertises* to a client that reached it over TLS.
 *
 * @see docs/protocol/0.11.0.md, section "Transport security (SSL)"
 */
#[CoversClass(SocketStream::class)]
#[CoversClass(SecurityProtocol::class)]
#[CoversClass(SslProtocol::class)]
#[CoversClass(ConnectionFactory::class)]
#[CoversClass(Cluster::class)]
#[CoversClass(Node::class)]
final class SslTransportTest extends IntegrationTestCase
{
    /**
     * Client id sent along with every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t5-ssl';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * A CA file that has signed nothing of this cluster, created once for the whole class
     */
    private static ?string $foreignCaFile = null;

    public static function tearDownAfterClass(): void
    {
        if (self::$foreignCaFile !== null && is_file(self::$foreignCaFile)) {
            unlink(self::$foreignCaFile);
        }
        self::$foreignCaFile = null;
    }

    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('openssl')) {
            self::markTestSkipped('The openssl extension is required for the SSL transport');
        }
        if (!is_readable(self::brokerCertificateFile())) {
            self::markTestSkipped('The certificate of the test broker is not available');
        }
    }

    protected function tearDown(): void
    {
        ConnectionFactory::closeAll();
    }

    /**
     * The endpoint a broker advertises depends on the listener the request arrived on.
     *
     * `KafkaApis.handleTopicMetadataRequest` @ 0.9.0.1 answers with `brokers.map(_.getBrokerEndPoint(request.securityProtocol))`,
     * and Metadata v0 has room for exactly one host/port per broker. A client that connects over TLS therefore
     * learns the TLS endpoints of the cluster and never falls back to the plaintext port.
     */
    public function testMetadataAnswersWithTheEndpointOfTheListenerItArrivedOn(): void
    {
        $overSsl       = $this->requestClusterMetadata($this->connectOverSsl(), 101);
        $overPlaintext = $this->requestClusterMetadata($this->connect(), 102);

        self::assertNotEmpty($overSsl->brokers);
        self::assertSame(
            array_keys($overPlaintext->brokers),
            array_keys($overSsl->brokers),
            'both listeners belong to the same brokers'
        );

        [$sslHost, $sslPort]             = self::hostAndPort(self::sslBootstrapServer());
        [, $plaintextPort]               = self::hostAndPort(self::firstBootstrapServer());
        $advertisedOverSsl               = reset($overSsl->brokers);
        $advertisedOverPlaintext         = reset($overPlaintext->brokers);

        self::assertSame($sslPort, $advertisedOverSsl->port, 'the SSL listener advertises its own port');
        self::assertSame($sslHost, $advertisedOverSsl->host);
        self::assertSame($plaintextPort, $advertisedOverPlaintext->port);
        self::assertNotSame(
            $advertisedOverPlaintext->port,
            $advertisedOverSsl->port,
            'the very same broker is advertised under two different endpoints'
        );
    }

    /**
     * The whole request/response cycle over TLS, with the broker validating the CRC of every appended message
     */
    public function testProduceAndFetchTravelThroughTheEncryptedChannel(): void
    {
        $topic = self::uniqueTopicName('t5-ssl');
        new TopicMetadataProbe(fn(): Stream => $this->connectOverSsl(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);

        $stream  = $this->connectOverSsl();
        $records = [[null, 'encrypted'], ['key', 'and authenticated']];

        new ProduceRequest(
            [$topic => [0 => SpecMessageSet::of($records)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            201
        )->writeTo($stream);

        $produced = ProduceResponse::unpack($stream);
        self::assertSame(201, $produced->getCorrelationId());
        self::assertSame(0, $produced->topics[$topic]->partitions[0]->errorCode);
        self::assertSame(0, $produced->topics[$topic]->partitions[0]->baseOffset);

        new FetchRequest([$topic => [0 => 0]], 1000, 1, 65536, -1, self::CLIENT_ID, 202)->writeTo($stream);

        $fetched   = FetchResponse::unpack($stream)->topics[$topic]->partitions[0];
        $messages  = $fetched->getMessageSet()->getRecords();
        $delivered = [];
        foreach ($messages as $message) {
            $delivered[] = [$message->key, $message->value];
        }

        self::assertSame(0, $fetched->errorCode);
        self::assertSame(2, $fetched->highWaterMarkOffset);
        self::assertSame($records, $delivered, 'the payload survives the encrypted channel unchanged');
    }

    /**
     * The SSL configuration has to survive metadata discovery: the connections that {@see Cluster} opens to the
     * brokers it learned about are opened from the very same configuration array, so they are encrypted as well.
     */
    public function testClusterDiscoveredOverSslKeepsTalkingSsl(): void
    {
        $topic = self::uniqueTopicName('t5-ssl-cluster');
        new TopicMetadataProbe(fn(): Stream => $this->connectOverSsl(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);

        $configuration = $this->sslConfiguration() + [
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::sslBootstrapServer()],
            ClientConfig::CLIENT_ID         => self::CLIENT_ID,
        ];
        $cluster       = Cluster::bootstrap($configuration);

        [, $sslPort] = self::hostAndPort(self::sslBootstrapServer());
        $leader      = $cluster->leaderFor($topic, 0);

        self::assertSame($sslPort, $leader->port, 'the leader was discovered through the SSL listener');

        $connection = $leader->getConnection($configuration);
        self::assertInstanceOf(SocketStream::class, $connection);
        self::assertSame(SecurityProtocol::SSL, $connection->getSecurityProtocol());

        // ... and that connection really answers, i.e. the handshake was performed on it as well
        new MetadataRequest([$topic], true, self::CLIENT_ID, 301)->writeTo($connection);
        self::assertSame(301, MetadataResponse::unpack($connection)->getCorrelationId());

        // A plaintext connection to the same broker is a different connection, never the cached encrypted one
        self::assertNotSame($connection, $leader->getConnection([ClientConfig::CLIENT_ID => self::CLIENT_ID]));
    }

    public function testCertificateOfTheBrokerIsRefusedUnderAnotherAuthority(): void
    {
        $stream = new SocketStream(
            'tcp://' . self::sslBootstrapServer(),
            [
                ClientConfig::SECURITY_PROTOCOL    => SecurityProtocol::SSL,
                ClientConfig::SSL_CA_CERT_LOCATION => self::foreignCaFile(),
            ],
            5.0
        );

        try {
            $stream->connect();
            self::fail('The self-signed certificate of the broker must not verify under a foreign authority');
        } catch (NetworkException $exception) {
            $context = $exception->getContext();
            self::assertStringContainsString('Failed to enable encryption', $context['error']);
            self::assertStringContainsString('certificate verify failed', $context['error']);
        }
    }

    /**
     * A Kafka broker speaks TLS from the first byte of a connection to its SSL listener: there is no in-protocol
     * upgrade, so a plaintext request is simply an unparseable handshake record and the broker drops the socket.
     */
    public function testPlaintextRequestOnTheSslListenerIsNotAnswered(): void
    {
        $stream = new SocketStream(
            'tcp://' . self::sslBootstrapServer(),
            [ClientConfig::REQUEST_TIMEOUT_MS => 5000],
            5.0
        );

        $this->expectException(NetworkException::class);

        new MetadataRequest([], true, self::CLIENT_ID, 401)->writeTo($stream);
        MetadataResponse::unpack($stream);
    }

    /**
     * Opens an encrypted connection to the SSL listener of the broker
     *
     * @param array<string, mixed> $configuration Extra configuration options
     */
    private function connectOverSsl(array $configuration = []): SocketStream
    {
        return new SocketStream(
            'tcp://' . self::sslBootstrapServer(),
            $configuration + $this->sslConfiguration(),
            5.0
        );
    }

    /**
     * The client configuration that talks to the SSL listener of the test broker
     *
     * @return array<string, mixed>
     */
    private function sslConfiguration(): array
    {
        return [
            ClientConfig::SECURITY_PROTOCOL    => SecurityProtocol::SSL,
            ClientConfig::SSL_CA_CERT_LOCATION => self::brokerCertificateFile(),
            ClientConfig::SEND_BUFFER_BYTES    => 131072,
            ClientConfig::RECEIVE_BUFFER_BYTES => 32768,
            ClientConfig::REQUEST_TIMEOUT_MS   => 10000,
        ];
    }

    /**
     * Asks the given connection for the metadata of the whole cluster
     */
    private function requestClusterMetadata(Stream $stream, int $correlationId): MetadataResponse
    {
        new MetadataRequest([], true, self::CLIENT_ID, $correlationId)->writeTo($stream);
        $response = MetadataResponse::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return $response;
    }

    /**
     * Splits a `host:port` pair
     *
     * @return array{string, int}
     */
    private static function hostAndPort(string $address): array
    {
        $separator = strrpos($address, ':');
        self::assertNotFalse($separator, "Malformed broker address {$address}");

        return [substr($address, 0, $separator), (int) substr($address, $separator + 1)];
    }

    /**
     * Creates a self-signed certificate that signed nothing, to be used as a trust anchor that must not match
     */
    private static function foreignCaFile(): string
    {
        if (self::$foreignCaFile !== null) {
            return self::$foreignCaFile;
        }

        $privateKey  = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
        $request     = openssl_csr_new(['commonName' => 'localhost'], $privateKey, ['digest_alg' => 'sha256']);
        $certificate = openssl_csr_sign($request, null, $privateKey, 365, ['digest_alg' => 'sha256']);
        if ($certificate === false || !openssl_x509_export($certificate, $exported)) {
            self::markTestSkipped('Can not generate a certificate with the openssl extension');
        }

        $fileName = (string) tempnam(sys_get_temp_dir(), 'kafka-foreign-ca-');
        file_put_contents($fileName, $exported);
        self::$foreignCaFile = $fileName;

        return $fileName;
    }
}
