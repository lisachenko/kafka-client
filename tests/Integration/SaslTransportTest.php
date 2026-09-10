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
use PHPUnit\Framework\Attributes\DataProvider;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\IllegalSaslStateException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationFailedException;
use Protocol\Kafka\Common\Errors\UnsupportedSaslMechanismException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SaslToken;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceResponseV2;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateRequest;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateResponse;
use Protocol\Kafka\Protocol\Request\SaslHandshakeRequest;
use Protocol\Kafka\Protocol\Request\SaslHandshakeRequestV0;
use Protocol\Kafka\Protocol\Request\SaslHandshakeResponse;
use Protocol\Kafka\Tests\Fixture\SpecMessageSet;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the SASL/PLAIN authentication against the SASL listeners of a real Kafka 1.1.1 broker.
 *
 * Kafka 0.10.0 (KIP-43) put the mechanism negotiation into the protocol - `SaslHandshake`, api key 17 - and added
 * the PLAIN mechanism, whose token is a user name and a password rather than a Kerberos ticket. Kafka 1.0
 * (KIP-152) put the tokens themselves into it, as `SaslAuthenticate` requests (api key 36) that a **v1** handshake
 * asks for, and with them an error code for a refused credential. What is tested here is both exchanges end to end
 * on both SASL listeners of the test broker: the handshake, the tokens in either framing, the ordinary traffic
 * afterwards, and every way a broker can refuse - the 58 with a message after a v1 handshake, and the connection
 * that simply goes away after a v0 one.
 *
 * @see docs/protocol/1.1.md, sections "SaslHandshake API (key 17, v0 and v1)" and "SaslAuthenticate API (key 36, v0)"
 * @see \Protocol\Kafka\Tests\Unit\IO\SocketStreamSaslTest for the same exchange against a scripted listener
 */
#[CoversClass(SocketStream::class)]
#[CoversClass(SecurityProtocol::class)]
#[CoversClass(SaslMechanism::class)]
#[CoversClass(SaslToken::class)]
#[CoversClass(SaslAuthenticateRequest::class)]
#[CoversClass(SaslAuthenticateResponse::class)]
#[CoversClass(SaslHandshakeRequest::class)]
#[CoversClass(SaslHandshakeRequestV0::class)]
#[CoversClass(SaslHandshakeResponse::class)]
#[CoversClass(ConnectionFactory::class)]
#[CoversClass(Cluster::class)]
#[CoversClass(Node::class)]
final class SaslTransportTest extends IntegrationTestCase
{
    /**
     * Client id sent along with every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t8-sasl';

    /**
     * Credentials of `docker/kafka-1.1.1/jaas.conf`
     */
    private const string USERNAME = 'kafkatest';

    private const string PASSWORD = 'kafkatest-secret';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    protected function setUp(): void
    {
        parent::setUp();

        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(
                self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, skipping the tests against a SASL listener'
            );
        }
    }

    protected function tearDown(): void
    {
        ConnectionFactory::closeAll();
    }

    /**
     * The whole request/response cycle over an authenticated connection, on both SASL listeners.
     *
     * `SASL_SSL` is the very same exchange inside the TLS channel: the handshake and the token are written after
     * `stream_socket_enable_crypto()` succeeded, which is what makes PLAIN sound at all - the credentials are in
     * clear text inside the token.
     */
    #[DataProvider('saslListeners')]
    public function testProduceAndFetchTravelThroughAnAuthenticatedConnection(string $listener): void
    {
        $topic = self::uniqueTopicName('t8-sasl');
        new TopicMetadataProbe(fn(): Stream => $this->connectWithSasl($listener), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);

        $stream  = $this->connectWithSasl($listener);
        $records = [[null, 'authenticated'], ['key', 'with SASL/PLAIN']];

        // The batch is a message set of the specification, which only a request below version 3 may carry: a
        // Produce v3 accepts the message format v2 alone, see docs/protocol/1.1.md
        new ProduceRequestV2(
            [$topic => [0 => SpecMessageSet::of($records)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            201
        )->writeTo($stream);

        $produced = ProduceResponseV2::unpack($stream);
        self::assertSame(201, $produced->getCorrelationId());
        self::assertSame(0, $produced->topics[$topic]->partitions[0]->errorCode);
        self::assertSame(0, $produced->topics[$topic]->partitions[0]->baseOffset);

        new FetchRequest([$topic => [0 => 0]], 1000, 1, 65536, -1, self::CLIENT_ID, 202)->writeTo($stream);

        $fetched   = FetchResponse::unpack($stream)->topics[$topic]->partitions[0];
        $delivered = [];
        foreach ($fetched->getRecords()->getRecords() as $message) {
            $delivered[] = [$message->key, $message->value];
        }

        self::assertSame(0, $fetched->errorCode);
        self::assertSame(2, $fetched->highWaterMarkOffset);
        self::assertSame($records, $delivered, 'the payload survives the authenticated channel unchanged');
        self::assertSame($listener, $stream->getSecurityProtocol());
        self::assertSame(SaslMechanism::PLAIN, $stream->getSaslMechanism());
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function saslListeners(): iterable
    {
        yield 'SASL over plaintext' => [SecurityProtocol::SASL_PLAINTEXT];
        yield 'SASL over TLS'       => [SecurityProtocol::SASL_SSL];
    }

    /**
     * The endpoint a broker advertises is the one of the listener the request arrived on - for the SASL listeners
     * as well, so a client that bootstraps over SASL discovers the endpoints it can authenticate to.
     */
    public function testMetadataAnswersWithTheEndpointOfTheListenerItArrivedOn(): void
    {
        $overSasl      = $this->requestClusterMetadata($this->connectWithSasl(SecurityProtocol::SASL_PLAINTEXT), 101);
        $overPlaintext = $this->requestClusterMetadata($this->connect(), 102);

        self::assertNotEmpty($overSasl->brokers);
        self::assertSame(
            array_keys($overPlaintext->brokers),
            array_keys($overSasl->brokers),
            'both listeners belong to the same brokers'
        );

        [$saslHost, $saslPort]   = self::hostAndPort(self::saslBootstrapServer());
        [, $plaintextPort]       = self::hostAndPort(self::firstBootstrapServer());
        $advertisedOverSasl      = reset($overSasl->brokers);
        $advertisedOverPlaintext = reset($overPlaintext->brokers);

        self::assertSame($saslPort, $advertisedOverSasl->port, 'the SASL listener advertises its own port');
        self::assertSame($saslHost, $advertisedOverSasl->host);
        self::assertNotSame(
            $advertisedOverPlaintext->port,
            $advertisedOverSasl->port,
            'the very same broker is advertised under two different endpoints'
        );
        self::assertSame($plaintextPort, $advertisedOverPlaintext->port);

        if (self::saslSslBootstrapServer() !== '') {
            $overSaslSsl = $this->requestClusterMetadata($this->connectWithSasl(SecurityProtocol::SASL_SSL), 103);
            [, $saslSslPort] = self::hostAndPort(self::saslSslBootstrapServer());

            self::assertSame($saslSslPort, reset($overSaslSsl->brokers)->port);
        }
    }

    /**
     * The configuration that discovered the cluster keeps authenticating every connection it opens afterwards
     */
    public function testClusterDiscoveredOverSaslKeepsAuthenticating(): void
    {
        $topic = self::uniqueTopicName('t8-sasl-cluster');
        new TopicMetadataProbe(
            fn(): Stream => $this->connectWithSasl(SecurityProtocol::SASL_PLAINTEXT),
            30.0,
            self::CLIENT_ID
        )->awaitTopicWithLeaders($topic);

        $configuration = $this->configurationFor(SecurityProtocol::SASL_PLAINTEXT) + [
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::CLIENT_ID         => self::CLIENT_ID,
        ];
        $cluster       = Cluster::bootstrap($configuration);

        [, $saslPort] = self::hostAndPort(self::saslBootstrapServer());
        $leader       = $cluster->leaderFor($topic, 0);

        self::assertSame($saslPort, $leader->port, 'the leader was discovered through the SASL listener');

        $connection = $leader->getConnection($configuration);
        self::assertInstanceOf(SocketStream::class, $connection);
        self::assertSame(SecurityProtocol::SASL_PLAINTEXT, $connection->getSecurityProtocol());

        // ... and that connection really answers, i.e. it was authenticated as well
        new MetadataRequest([$topic], true, self::CLIENT_ID, 301)->writeTo($connection);
        self::assertSame(301, MetadataResponse::unpack($connection)->getCorrelationId());

        // A connection for other credentials is a different connection, never the cached authenticated one
        $otherUser = $leader->getConnection(
            [ClientConfig::SASL_USERNAME => 'admin', ClientConfig::SASL_PASSWORD => 'admin-secret'] + $configuration
        );
        self::assertNotSame($connection, $otherUser);
    }

    /**
     * The framed exchange of Kafka 1.0, driven by hand over both SASL listeners: a v1 handshake, the very same
     * PLAIN token inside a `SaslAuthenticate` request, and an answer that says so with an error code.
     */
    #[DataProvider('saslListeners')]
    public function testTokenOfAVersionOneHandshakeTravelsInsideASaslAuthenticateRequest(string $listener): void
    {
        $stream = $this->connectWithoutAuthentication($listener);

        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 431)->writeTo($stream);
        $handshake = SaslHandshakeResponse::unpack($stream);

        self::assertSame(431, $handshake->getCorrelationId());
        self::assertSame(0, $handshake->errorCode);
        self::assertSame([SaslMechanism::PLAIN], $handshake->enabledMechanisms);

        $token = SaslToken::ofPlainCredentials(self::USERNAME, self::PASSWORD);
        new SaslAuthenticateRequest($token->token, self::CLIENT_ID, 432)->writeTo($stream);
        $answer = SaslAuthenticateResponse::unpack($stream);

        self::assertSame(432, $answer->getCorrelationId());
        self::assertSame(0, $answer->errorCode, 'the credentials of the container are accepted');
        self::assertNull($answer->errorMessage, 'a successful answer carries no message');
        self::assertSame('', $answer->saslAuthBytes, 'PLAIN completes with the empty token');

        // ... and the connection is an ordinary one from here on
        new MetadataRequest([], true, self::CLIENT_ID, 433)->writeTo($stream);
        self::assertSame(433, MetadataResponse::unpack($stream)->getCorrelationId());
    }

    /**
     * Wrong credentials are the error code 58 with a message from Kafka 1.0 on - and the answer is the last frame
     * of the connection, which the broker closes right after it.
     */
    #[DataProvider('refusedCredentials')]
    public function testRefusedCredentialsAreAnsweredWithTheErrorCode58(
        string $username,
        string $password,
        string $expectedMessage
    ): void {
        $stream = new SocketStream(
            'tcp://' . self::saslBootstrapServer(),
            [
                ClientConfig::SASL_USERNAME => $username,
                ClientConfig::SASL_PASSWORD => $password,
            ] + $this->configurationFor(SecurityProtocol::SASL_PLAINTEXT),
            5.0
        );

        try {
            $stream->connect();
            self::fail('The broker must not authenticate these credentials');
        } catch (SaslAuthenticationException $exception) {
            $context = $exception->getContext();
            self::assertSame(KafkaException::SASL_AUTHENTICATION_FAILED, $context['errorCode']);
            self::assertSame($expectedMessage, $context['errorMessage']);
            self::assertSame($expectedMessage, $context['error']);
            self::assertSame($username, $context['username']);
            self::assertNotContains($password, $context, 'the password never reaches an exception');
            self::assertInstanceOf(
                SaslAuthenticationFailedException::class,
                $exception->getPrevious(),
                'the wire code of the answer is the cause of the client-side exception'
            );
        }

        self::assertFalse($stream->isConnected(), 'the connection is dropped with the failed authentication');
    }

    /**
     * @return iterable<string, array{string, string, string}>
     */
    public static function refusedCredentials(): iterable
    {
        $invalid = 'Authentication failed: Invalid username or password';

        yield 'wrong password' => [self::USERNAME, 'not-the-password', $invalid];
        yield 'unknown user'   => ['t8-nobody', self::PASSWORD, $invalid];
    }

    /**
     * A token the mechanism cannot even parse - PLAIN needs three NUL-separated parts - is the same error code
     * with the other message of `SaslServerAuthenticator`.
     */
    public function testTokenTheMechanismCanNotParseIsAnsweredWithTheErrorCode58(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 441)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);

        new SaslAuthenticateRequest('no-separators-at-all', self::CLIENT_ID, 442)->writeTo($stream);
        $answer = SaslAuthenticateResponse::unpack($stream);

        self::assertSame(KafkaException::SASL_AUTHENTICATION_FAILED, $answer->errorCode);
        self::assertSame(
            'Authentication failed due to invalid credentials with SASL mechanism PLAIN',
            $answer->errorMessage
        );
        self::assertSame('', $answer->saslAuthBytes);
    }

    /**
     * A second `SaslAuthenticate` reaches `KafkaApis` instead of the authenticator: it is answered with 34 and,
     * unlike every other refusal of the exchange, it leaves the connection usable.
     */
    public function testSecondSaslAuthenticateIsAnsweredWithTheIllegalSaslState(): void
    {
        $stream = $this->connectPlain();
        $token  = SaslToken::ofPlainCredentials(self::USERNAME, self::PASSWORD);

        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 451)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);
        new SaslAuthenticateRequest($token->token, self::CLIENT_ID, 452)->writeTo($stream);
        self::assertSame(0, SaslAuthenticateResponse::unpack($stream)->errorCode);

        new SaslAuthenticateRequest($token->token, self::CLIENT_ID, 453)->writeTo($stream);
        $answer = SaslAuthenticateResponse::unpack($stream);

        self::assertSame(KafkaException::ILLEGAL_SASL_STATE, $answer->errorCode);
        self::assertSame('SaslAuthenticate request received after successful authentication', $answer->errorMessage);
        self::assertInstanceOf(IllegalSaslStateException::class, KafkaException::fromCode($answer->errorCode));

        // The connection survives it, which no other refusal of this exchange does
        new MetadataRequest([], true, self::CLIENT_ID, 454)->writeTo($stream);
        self::assertSame(454, MetadataResponse::unpack($stream)->getCorrelationId());
    }

    /**
     * The raw exchange of the lines up to 0.11 is still served, unchanged, by a 1.1.1 broker: a v0 handshake and
     * bare token frames with no header at all.
     */
    public function testTheRawTokenExchangeOfAVersionZeroHandshakeStillWorks(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequestV0(SaslMechanism::PLAIN, self::CLIENT_ID, 461)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);

        SaslToken::ofPlainCredentials(self::USERNAME, self::PASSWORD)->writeTo($stream);
        self::assertTrue(SaslToken::readFrom($stream)->isEmpty(), 'PLAIN completes with the empty token');

        new MetadataRequest([], true, self::CLIENT_ID, 462)->writeTo($stream);
        self::assertSame(462, MetadataResponse::unpack($stream)->getCorrelationId());
    }

    /**
     * ... and it still has no error code either: refused credentials are a closed connection there
     */
    public function testRefusedCredentialsOfTheRawExchangeCloseTheConnection(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequestV0(SaslMechanism::PLAIN, self::CLIENT_ID, 471)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);

        $this->expectException(NetworkException::class);
        SaslToken::ofPlainCredentials(self::USERNAME, 'not-the-password')->writeTo($stream);
        SaslToken::readFrom($stream);
    }

    /**
     * The two halves have to match: the broker switches on the version of the handshake and on nothing else, so
     * the wrong framing is read as garbage and the connection is closed without an answer.
     */
    public function testAFramedTokenAfterAVersionZeroHandshakeClosesTheConnection(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequestV0(SaslMechanism::PLAIN, self::CLIENT_ID, 481)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);

        $this->expectException(NetworkException::class);
        $token = SaslToken::ofPlainCredentials(self::USERNAME, self::PASSWORD);
        new SaslAuthenticateRequest($token->token, self::CLIENT_ID, 482)->writeTo($stream);
        SaslAuthenticateResponse::unpack($stream);
    }

    public function testARawTokenAfterAVersionOneHandshakeClosesTheConnection(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 491)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);

        $this->expectException(NetworkException::class);
        SaslToken::ofPlainCredentials(self::USERNAME, self::PASSWORD)->writeTo($stream);
        SaslToken::readFrom($stream);
    }

    /**
     * `SaslAuthenticate` belongs to the authentication phase: before the handshake the authenticator does not even
     * build an error response for it, it closes the connection.
     */
    public function testSaslAuthenticateBeforeTheHandshakeIsNotAnswered(): void
    {
        $stream = $this->connectPlain();
        $token  = SaslToken::ofPlainCredentials(self::USERNAME, self::PASSWORD);

        $this->expectException(NetworkException::class);
        new SaslAuthenticateRequest($token->token, self::CLIENT_ID, 501)->writeTo($stream);
        SaslAuthenticateResponse::unpack($stream);
    }

    /**
     * A mechanism the broker has not enabled is refused with the error code 33 and the list of the enabled ones,
     * and the broker closes the connection right afterwards - in both versions of the handshake.
     *
     * The request is written by hand here because the client refuses to configure a mechanism it cannot perform.
     */
    public function testMechanismTheBrokerDoesNotHaveIsAnsweredWithErrorCode33(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequest(SaslMechanism::GSSAPI, self::CLIENT_ID, 401)->writeTo($stream);
        $response = SaslHandshakeResponse::unpack($stream);

        self::assertSame(401, $response->getCorrelationId());
        self::assertSame(33, $response->errorCode);
        self::assertSame([SaslMechanism::PLAIN], $response->enabledMechanisms);
        self::assertInstanceOf(
            UnsupportedSaslMechanismException::class,
            KafkaException::fromCode($response->errorCode)
        );

        // The broker gives no second chance on that connection
        $this->expectException(NetworkException::class);
        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 402)->writeTo($stream);
        SaslHandshakeResponse::unpack($stream);
    }

    /**
     * The handshake is answered exactly once. After a **v1** one the second frame is parsed as a Kafka request, so
     * the broker answers 34 - with an empty mechanism list, which is a change of Kafka 1.1 - and closes.
     */
    public function testSecondHandshakeOfAVersionOneConnectionIsAnsweredWithTheIllegalSaslState(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 411)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);

        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 412)->writeTo($stream);
        $second = SaslHandshakeResponse::unpack($stream);

        self::assertSame(KafkaException::ILLEGAL_SASL_STATE, $second->errorCode);
        self::assertSame([], $second->enabledMechanisms, 'a 1.1 broker answers that with no mechanism at all');

        // ... and nothing else comes out of that connection
        $this->expectException(NetworkException::class);
        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 413)->writeTo($stream);
        SaslHandshakeResponse::unpack($stream);
    }

    /**
     * After a **v0** handshake the second frame is a token, not a request, so there is nothing to answer with
     */
    public function testSecondHandshakeOfAVersionZeroConnectionIsNotAnswered(): void
    {
        $stream = $this->connectPlain();

        new SaslHandshakeRequestV0(SaslMechanism::PLAIN, self::CLIENT_ID, 421)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode);

        $this->expectException(NetworkException::class);
        new SaslHandshakeRequestV0(SaslMechanism::PLAIN, self::CLIENT_ID, 422)->writeTo($stream);
        SaslHandshakeResponse::unpack($stream);
    }

    /**
     * A handshake that never reaches the authenticator - here on the PLAINTEXT listener - is the same 34 with the
     * empty mechanism list, but it comes from `KafkaApis` and leaves the connection alone.
     */
    public function testHandshakeOnAListenerThatDoesNotAuthenticateIsAnsweredWithTheIllegalSaslState(): void
    {
        $stream = new SocketStream(
            'tcp://' . self::firstBootstrapServer(),
            [ClientConfig::REQUEST_TIMEOUT_MS => 5000],
            5.0
        );

        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 511)->writeTo($stream);
        $response = SaslHandshakeResponse::unpack($stream);

        self::assertSame(KafkaException::ILLEGAL_SASL_STATE, $response->errorCode);
        self::assertSame([], $response->enabledMechanisms, 'Kafka 1.1 empties that list, 1.0.2 still filled it');

        // The api layer answered, so the connection is untouched
        new MetadataRequest([], true, self::CLIENT_ID, 512)->writeTo($stream);
        self::assertSame(512, MetadataResponse::unpack($stream)->getCorrelationId());
    }

    /**
     * An ordinary request as the first frame of a SASL connection is dropped without an answer: the broker parses
     * it as a handshake request - or as the GSSAPI token that a 0.9 client would send there - and gives up.
     */
    public function testKafkaRequestBeforeTheHandshakeIsNotAnswered(): void
    {
        $stream = $this->connectPlain();

        $this->expectException(NetworkException::class);

        new MetadataRequest([], true, self::CLIENT_ID, 421)->writeTo($stream);
        MetadataResponse::unpack($stream);
    }

    /**
     * ApiVersions is the one exception: a broker answers it before the authentication (KIP-35), so that a client
     * can learn what the broker speaks before it decides how to talk to it - which is exactly how the Java client
     * of 1.1 finds out whether it may promise the framed exchange.
     *
     * The request is written from the primitives of the stream because the ApiVersions message classes belong to
     * another ticket of this line; only its header is needed here.
     */
    public function testApiVersionsIsAnsweredBeforeTheHandshake(): void
    {
        $stream = $this->connectPlain();

        $clientId = self::CLIENT_ID;
        $stream->writeInt32(2 + 2 + 4 + 2 + strlen($clientId));
        $stream->writeInt16(ApiKeys::API_VERSIONS);
        $stream->writeInt16(0);
        $stream->writeInt32(431);
        $stream->writeString($clientId);

        $messageSize = $stream->readInt32();
        self::assertGreaterThan(10, $messageSize);
        self::assertSame(431, $stream->readInt32(), 'the answer carries the correlation id of the request');
        self::assertSame(0, $stream->readInt16(), 'the error code of the ApiVersions response');
        self::assertGreaterThan(0, $stream->readInt32(), 'the broker lists the api versions it serves');

        // The rest of the answer is of no interest here, but the connection has to stay usable for the handshake
        $stream->read('a' . ($messageSize - 4 - 2 - 4));
        new SaslHandshakeRequest(SaslMechanism::PLAIN, self::CLIENT_ID, 432)->writeTo($stream);
        self::assertSame(0, SaslHandshakeResponse::unpack($stream)->errorCode, 'the probe costs nothing');
    }

    /**
     * Opens an authenticated connection to the SASL listener of the given security protocol
     */
    private function connectWithSasl(string $securityProtocol): SocketStream
    {
        return new SocketStream(
            'tcp://' . $this->listenerFor($securityProtocol),
            $this->configurationFor($securityProtocol),
            5.0
        );
    }

    /**
     * Opens an unauthenticated connection to the SASL_PLAINTEXT listener, to drive the exchange by hand
     */
    private function connectPlain(): SocketStream
    {
        return new SocketStream(
            'tcp://' . self::saslBootstrapServer(),
            [ClientConfig::REQUEST_TIMEOUT_MS => 5000],
            5.0
        );
    }

    /**
     * Opens a connection to a SASL listener **without** authenticating it, so that the exchange can be driven by
     * hand: `SASL_SSL` needs the TLS channel around it, which is the `SSL` transport of this client and nothing
     * more - the broker performs the TLS handshake before it expects a single Kafka byte either way.
     */
    private function connectWithoutAuthentication(string $securityProtocol): SocketStream
    {
        $listener      = $this->listenerFor($securityProtocol);
        $configuration = [ClientConfig::REQUEST_TIMEOUT_MS => 5000];

        if ($securityProtocol === SecurityProtocol::SASL_SSL) {
            $configuration += [
                ClientConfig::SECURITY_PROTOCOL      => SecurityProtocol::SSL,
                ClientConfig::SSL_CA_CERT_LOCATION   => self::saslBrokerCertificateFile(),
            ];
        }

        return new SocketStream('tcp://' . $listener, $configuration, 5.0);
    }

    /**
     * The client configuration that authenticates against the listener of the given security protocol
     *
     * @return array<string, mixed>
     */
    private function configurationFor(string $securityProtocol): array
    {
        $configuration = [
            ClientConfig::SECURITY_PROTOCOL  => $securityProtocol,
            ClientConfig::SASL_MECHANISM     => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME      => self::USERNAME,
            ClientConfig::SASL_PASSWORD      => self::PASSWORD,
            ClientConfig::CLIENT_ID          => self::CLIENT_ID,
            ClientConfig::SEND_BUFFER_BYTES  => 131072,
            ClientConfig::REQUEST_TIMEOUT_MS => 10000,
        ];

        if ($securityProtocol === SecurityProtocol::SASL_SSL) {
            $configuration[ClientConfig::SSL_CA_CERT_LOCATION] = self::saslBrokerCertificateFile();
        }

        return $configuration;
    }

    /**
     * Returns the listener of the given security protocol, skipping the test when it is not configured
     */
    private function listenerFor(string $securityProtocol): string
    {
        if ($securityProtocol !== SecurityProtocol::SASL_SSL) {
            return self::saslBootstrapServer();
        }

        if (self::saslSslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_SSL_BOOTSTRAP_SERVERS_ENV . ' is not set');
        }
        if (!extension_loaded('openssl')) {
            self::markTestSkipped('The openssl extension is required for security.protocol = SASL_SSL');
        }
        if (!is_readable(self::saslBrokerCertificateFile())) {
            self::markTestSkipped('The certificate of the test broker is not available');
        }

        return self::saslSslBootstrapServer();
    }

    /**
     * The certificate the 0.10.2.2 test broker presents on its SSL and SASL_SSL listeners
     */
    private static function saslBrokerCertificateFile(): string
    {
        return dirname(__DIR__, 2) . '/docker/kafka-1.1.1/ssl/broker.crt';
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
}
