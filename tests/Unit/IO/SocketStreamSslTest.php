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

namespace Protocol\Kafka\Tests\Unit\IO;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Common\Security\SslProtocol;
use Protocol\Kafka\IO\AbstractStream;
use Protocol\Kafka\IO\SocketStream;

/**
 * Tests the TLS transport of the socket stream against a local TLS server.
 *
 * The certificate is the one the test broker presents on its SSL listener (`docker/kafka-0.10.2.2/ssl`), so the
 * handshake performed here is the very same one the integration suite performs against Kafka.
 *
 * @see \Protocol\Kafka\Tests\Integration\SslTransportTest for the same handshake against a real broker
 * @see docs/protocol/0.10.2.md, section "Transport security (SSL)"
 */
#[CoversClass(SocketStream::class)]
#[CoversClass(AbstractStream::class)]
#[CoversClass(SecurityProtocol::class)]
#[CoversClass(SslProtocol::class)]
final class SocketStreamSslTest extends TestCase
{
    private ?LocalTlsServer $server = null;

    private ?SocketStream $stream = null;

    /**
     * A CA file that has nothing to do with the certificate of the server, created once for the whole class
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
        if (!extension_loaded('openssl')) {
            self::markTestSkipped('The openssl extension is required for the SSL transport');
        }
        if (!is_readable(self::certificateFile()) || !is_readable(self::keyFile())) {
            self::markTestSkipped('The test certificate of docker/kafka-0.10.2.2/ssl is not available');
        }
    }

    protected function tearDown(): void
    {
        $this->stream = null;
        $this->server?->stop();
        $this->server = null;
    }

    public function testPlaintextIsTheDefaultTransport(): void
    {
        self::assertSame(SecurityProtocol::PLAINTEXT, new SocketStream('tcp://127.0.0.1:9092')->getSecurityProtocol());
        self::assertSame(
            SecurityProtocol::SSL,
            new SocketStream(
                'tcp://127.0.0.1:9093',
                [ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SSL]
            )->getSecurityProtocol()
        );
    }

    /**
     * The TLS half of `SASL_SSL` is the very same handshake, performed before the SASL exchange
     *
     * @see \Protocol\Kafka\Tests\Unit\IO\SocketStreamSaslTest for the authentication that follows it
     */
    public function testSaslOverTlsEncryptsTheChannelAsWell(): void
    {
        self::assertTrue(SecurityProtocol::isEncrypted(SecurityProtocol::SASL_SSL));
        self::assertFalse(SecurityProtocol::isEncrypted(SecurityProtocol::SASL_PLAINTEXT));
    }

    #[DataProvider('inaccessibleFiles')]
    public function testInaccessibleCertificateMaterialIsReported(string $option, string $expectedMessage): void
    {
        $stream = new SocketStream('tcp://127.0.0.1:1', [
            ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SSL,
            $option                         => '/nonexistent/kafka-client.pem',
        ], 1.0);

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);

        $stream->connect();
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function inaccessibleFiles(): iterable
    {
        yield 'ca file'            => [ClientConfig::SSL_CA_CERT_LOCATION, 'CA file /nonexistent'];
        yield 'client certificate' => [ClientConfig::SSL_CLIENT_CERT_LOCATION, 'Client certificate file /nonexistent'];
        yield 'private key'        => [ClientConfig::SSL_KEY_LOCATION, 'Key file /nonexistent'];
    }

    public function testUnknownSslProtocolIsRejected(): void
    {
        $stream = new SocketStream('tcp://127.0.0.1:1', [
            ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SSL,
            ClientConfig::SSL_PROTOCOL      => 'TLSv1.3',
        ], 1.0);

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('SSL protocol TLSv1.3 is not implemented.');

        $stream->connect();
    }

    public function testPayloadTravelsThroughTheEncryptedChannel(): void
    {
        $stream = $this->connectToTlsServer();

        $stream->writeInt32(1);
        $stream->writeString('test');

        // The echo server sends every byte back, so what arrives is what the stream encrypted
        self::assertSame(1, $stream->readInt32());
        self::assertSame('test', $stream->readString());
        self::assertTrue($stream->isConnected());
    }

    public function testHandshakeIsNarrowedByTheEnabledProtocols(): void
    {
        $stream = $this->connectToTlsServer([
            ClientConfig::SSL_ENABLED_PROTOCOLS => [SslProtocol::TLSv1_2],
        ]);

        $stream->write('a4', 'ping');
        self::assertSame('ping', $stream->readRaw(4));
    }

    public function testCertificateSignedByAnotherAuthorityIsRefused(): void
    {
        $stream = $this->tlsStream([ClientConfig::SSL_CA_CERT_LOCATION => self::foreignCaFile()]);

        try {
            $stream->connect();
            self::fail('A certificate that the configured CA did not sign must not be accepted');
        } catch (NetworkException $exception) {
            $context = $exception->getContext();
            self::assertStringContainsString('Failed to enable encryption', $context['error']);
            self::assertStringContainsString('certificate verify failed', $context['error']);
        }
    }

    public function testHandshakeAgainstAPlaintextListenerFails(): void
    {
        $plainServer = stream_socket_server('tcp://127.0.0.1:0', $errorNumber, $errorString);
        if ($plainServer === false) {
            self::markTestSkipped("Can not listen on a local TCP port: {$errorString}");
        }

        $stream = new SocketStream(
            'tcp://' . stream_socket_get_name($plainServer, false),
            [
                ClientConfig::SECURITY_PROTOCOL    => SecurityProtocol::SSL,
                ClientConfig::SSL_CA_CERT_LOCATION => self::certificateFile(),
            ],
            // The handshake is bounded by the connection timeout, not by request.timeout.ms
            0.5
        );

        try {
            $stream->connect();
            self::fail('A plaintext listener can not complete a TLS handshake');
        } catch (NetworkException $exception) {
            self::assertStringContainsString('Failed to enable encryption', $exception->getContext()['error']);
        } finally {
            fclose($plainServer);
        }
    }

    /**
     * Opens an encrypted stream to a freshly started local TLS server
     *
     * @param array<string, mixed> $configuration Extra configuration options
     */
    private function connectToTlsServer(array $configuration = []): SocketStream
    {
        $stream = $this->tlsStream($configuration);
        $stream->connect();

        return $stream;
    }

    /**
     * Creates - but does not connect - a stream to a freshly started local TLS server
     *
     * @param array<string, mixed> $configuration Extra configuration options
     */
    private function tlsStream(array $configuration = []): SocketStream
    {
        $this->server = new LocalTlsServer(self::certificateFile(), self::keyFile());
        $this->stream = new SocketStream(
            'tcp://' . $this->server->address(),
            $configuration + [
                ClientConfig::SECURITY_PROTOCOL    => SecurityProtocol::SSL,
                ClientConfig::SSL_CA_CERT_LOCATION => self::certificateFile(),
                ClientConfig::REQUEST_TIMEOUT_MS   => 5000,
            ],
            5.0
        );

        return $this->stream;
    }

    /**
     * The self-signed certificate the test broker presents, valid for `localhost` and `127.0.0.1`
     */
    private static function certificateFile(): string
    {
        return dirname(__DIR__, 3) . '/docker/kafka-0.10.2.2/ssl/broker.crt';
    }

    private static function keyFile(): string
    {
        return dirname(__DIR__, 3) . '/docker/kafka-0.10.2.2/ssl/broker.key';
    }

    /**
     * Creates a self-signed certificate that has signed nothing, to be used as a trust anchor that must not match
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
