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
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationException;
use Protocol\Kafka\Common\Errors\UnsupportedSaslMechanismException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SaslToken;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\IO\AbstractStream;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\Request\SaslHandshakeRequest;
use Protocol\Kafka\Protocol\Request\SaslHandshakeResponse;

/**
 * Tests the SASL/PLAIN exchange of the socket stream against a scripted listener.
 *
 * Kafka 0.10.0 (KIP-43) put the authentication inside the protocol: a `SaslHandshake` request names the mechanism
 * and the tokens of that mechanism follow as bare size-prefixed frames. What is checked here is exactly that
 * conversation - the frames the client puts on the wire and what it makes of every answer a broker can give -
 * while {@see \Protocol\Kafka\Tests\Integration\SaslTransportTest} runs it against a real 0.10.2.2 broker.
 *
 * @see docs/protocol/0.10.2.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 */
#[CoversClass(SocketStream::class)]
#[CoversClass(AbstractStream::class)]
#[CoversClass(SecurityProtocol::class)]
#[CoversClass(SaslMechanism::class)]
#[CoversClass(SaslToken::class)]
#[CoversClass(SaslHandshakeRequest::class)]
#[CoversClass(SaslHandshakeResponse::class)]
final class SocketStreamSaslTest extends TestCase
{
    /**
     * Credentials of the scripted listener, the ones of the test broker
     */
    private const string USERNAME = 'kafkatest';

    private const string PASSWORD = 'kafkatest-secret';

    private ?LocalSaslServer $server = null;

    private ?SocketStream $stream = null;

    protected function tearDown(): void
    {
        $this->stream = null;
        $this->server?->stop();
        $this->server = null;
    }

    public function testSaslTransportsAreImplementedFromThisBranchOn(): void
    {
        self::assertSame(
            [SecurityProtocol::PLAINTEXT, SecurityProtocol::SSL, 'SASL_PLAINTEXT', 'SASL_SSL'],
            SecurityProtocol::implemented(),
            'Kafka 0.10.0 added the SaslHandshake request, which is what makes SASL implementable here'
        );
        self::assertSame(SecurityProtocol::all(), SecurityProtocol::implemented());
    }

    public function testUnknownTransportIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Unknown security protocol TLS, expected one of: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.'
        );

        new SocketStream('tcp://127.0.0.1:9092', [ClientConfig::SECURITY_PROTOCOL => 'TLS']);
    }

    /**
     * The whole exchange: the handshake request, the token, and a connection that carries ordinary bytes afterwards
     */
    public function testPlainCredentialsAreExchangedRightAfterTheConnection(): void
    {
        $stream = $this->connectTo(LocalSaslServer::SCENARIO_ACCEPT);

        self::assertTrue($stream->isConnected());
        self::assertSame(SaslMechanism::PLAIN, $stream->getSaslMechanism());

        [$handshake, $token] = $this->server?->receivedFrames() ?? [];

        // The handshake is an ordinary request: Size, ApiKey 17, ApiVersion 0, CorrelationId, ClientId, Mechanism
        self::assertSame('0011', substr($handshake, 8, 4), 'the api key of the SaslHandshake request');
        self::assertSame('0000', substr($handshake, 12, 4), 'a 0.10.2.2 broker serves version 0 only');
        self::assertStringEndsWith('0007' . bin2hex('t8-unit') . '0005' . bin2hex('PLAIN'), $handshake);

        // The token is not a request at all: a length prefix and `\0username\0password`
        self::assertSame(
            '0000001b' . bin2hex("\0" . self::USERNAME . "\0" . self::PASSWORD),
            $token,
            'the PLAIN token travels without a header, an api key or a correlation id'
        );

        // ... and the connection is usable for ordinary traffic afterwards
        $stream->writeInt32(42);
        self::assertSame(42, $stream->readInt32());
    }

    public function testAuthorizationIdOfThePlainTokenIsLeftEmpty(): void
    {
        $this->connectTo(LocalSaslServer::SCENARIO_ACCEPT);

        $token = (string) hex2bin($this->server?->receivedFrames()[1] ?? '');

        self::assertSame("\0", substr($token, 4, 1), 'the token starts with the empty authorization id');
    }

    /**
     * A broker that does not have the mechanism enabled answers the handshake with the error code 33 and the list
     * of the mechanisms it does have, and closes the connection right afterwards.
     */
    public function testRefusedMechanismIsReportedWithTheMechanismsOfTheBroker(): void
    {
        try {
            $this->connectTo(LocalSaslServer::SCENARIO_UNSUPPORTED_MECHANISM);
            self::fail('A refused mechanism must not produce a usable connection');
        } catch (UnsupportedSaslMechanismException $exception) {
            $context = $exception->getContext();
            self::assertSame(SaslMechanism::PLAIN, $context['mechanism']);
            self::assertSame([SaslMechanism::GSSAPI], $context['enabledMechanisms']);
        }

        self::assertFalse($this->stream?->isConnected(), 'the failed connection is dropped');
    }

    /**
     * Wrong credentials have no error code before Kafka 1.0: the broker closes the connection during the token
     * exchange, and the client must not read that as a network glitch it can retry.
     */
    public function testRefusedCredentialsAreReportedAsAnAuthenticationFailure(): void
    {
        try {
            $this->connectTo(LocalSaslServer::SCENARIO_REFUSE_CREDENTIALS);
            self::fail('Refused credentials must not produce a usable connection');
        } catch (SaslAuthenticationException $exception) {
            $context = $exception->getContext();
            self::assertStringContainsString('closed the connection', $context['error']);
            self::assertSame(SaslMechanism::PLAIN, $context['mechanism']);
            self::assertSame(self::USERNAME, $context['username']);
            self::assertArrayNotHasKey('password', $context, 'the password never reaches an exception');
        }

        self::assertFalse($this->stream?->isConnected());
    }

    public function testAnswerThatIsNotTheEmptyTokenIsRefused(): void
    {
        $this->expectException(SaslAuthenticationException::class);
        $this->expectExceptionMessage('SASL authentication');

        $this->connectTo(LocalSaslServer::SCENARIO_UNEXPECTED_TOKEN);
    }

    public function testHandshakeAnswerOfAnotherRequestIsRefused(): void
    {
        $this->expectException(CorrelationIdMismatchException::class);

        $this->connectTo(LocalSaslServer::SCENARIO_WRONG_CORRELATION_ID);
    }

    /**
     * A mechanism a 0.10.2.2 broker knows but this client does not perform is refused before a socket is opened,
     * with the reason it is not implemented for.
     */
    #[DataProvider('unimplementedMechanisms')]
    public function testUnimplementedMechanismIsRejectedWithItsReason(string $mechanism, string $reason): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($reason);

        new SocketStream('tcp://127.0.0.1:9094', $this->configuration([ClientConfig::SASL_MECHANISM => $mechanism]));
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function unimplementedMechanisms(): iterable
    {
        yield 'Kerberos' => [SaslMechanism::GSSAPI, 'PHP has no GSS-API binding'];
        yield 'SCRAM with SHA-256' => [SaslMechanism::SCRAM_SHA_256, 'multi-round exchange of RFC 5802'];
        yield 'SCRAM with SHA-512' => [SaslMechanism::SCRAM_SHA_512, 'multi-round exchange of RFC 5802'];
        yield 'a mechanism no broker knows' => ['OAUTHBEARER', 'only knows GSSAPI, PLAIN'];
    }

    #[DataProvider('incompleteCredentials')]
    public function testIncompleteCredentialsAreRejected(array $credentials, string $expectedOption): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage("needs a non-empty {$expectedOption}");

        new SocketStream('tcp://127.0.0.1:9094', $credentials + [
            ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SASL_PLAINTEXT,
        ]);
    }

    /**
     * @return iterable<string, array{array<string, mixed>, string}>
     */
    public static function incompleteCredentials(): iterable
    {
        yield 'nothing configured' => [[], ClientConfig::SASL_USERNAME];
        yield 'no password' => [[ClientConfig::SASL_USERNAME => 'kafkatest'], ClientConfig::SASL_PASSWORD];
        yield 'empty user name' => [
            [ClientConfig::SASL_USERNAME => '', ClientConfig::SASL_PASSWORD => 'secret'],
            ClientConfig::SASL_USERNAME,
        ];
        yield 'empty password' => [
            [ClientConfig::SASL_USERNAME => 'kafkatest', ClientConfig::SASL_PASSWORD => ''],
            ClientConfig::SASL_PASSWORD,
        ];
    }

    /**
     * The `sasl.*` options are meaningless for a transport that does not authenticate, and are ignored there
     */
    public function testCredentialsAreNotDemandedByATransportThatDoesNotAuthenticate(): void
    {
        $stream = new SocketStream('tcp://127.0.0.1:9092', [
            ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::PLAINTEXT,
            ClientConfig::SASL_MECHANISM    => SaslMechanism::GSSAPI,
        ]);

        self::assertSame(SecurityProtocol::PLAINTEXT, $stream->getSecurityProtocol());
        self::assertSame(SaslMechanism::GSSAPI, $stream->getSaslMechanism());
    }

    /**
     * Opens a stream against a listener playing the given scenario and forces the connection
     */
    private function connectTo(string $scenario): SocketStream
    {
        $this->server = new LocalSaslServer($scenario);
        $this->stream = new SocketStream(
            'tcp://' . $this->server->address(),
            $this->configuration(),
            5.0
        );
        $this->stream->connect();

        return $this->stream;
    }

    /**
     * The configuration of a client that authenticates with the credentials of the test broker
     *
     * @param array<string, mixed> $overrides Options that replace the defaults below
     *
     * @return array<string, mixed>
     */
    private function configuration(array $overrides = []): array
    {
        return $overrides + [
            ClientConfig::SECURITY_PROTOCOL  => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM     => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME      => self::USERNAME,
            ClientConfig::SASL_PASSWORD      => self::PASSWORD,
            ClientConfig::CLIENT_ID          => 't8-unit',
            ClientConfig::REQUEST_TIMEOUT_MS => 5000,
        ];
    }
}
