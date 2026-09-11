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

/**
 * @author Alexander.Lisachenko
 * @date   26.07.2016
 */

namespace Protocol\Kafka\IO;

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SaslToken;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Common\Security\SslProtocol;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateRequest;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateResponse;
use Protocol\Kafka\Protocol\Request\SaslHandshakeRequest;
use Protocol\Kafka\Protocol\Request\SaslHandshakeRequestV0;
use Protocol\Kafka\Protocol\Request\SaslHandshakeResponse;

/**
 * Implementation of the binary stream on top of a TCP socket, optionally wrapped in TLS and authenticated with SASL.
 *
 * Kafka 0.9.0.0 introduced listeners per security protocol: the very same request bytes are exchanged whatever the
 * listener is, so the transport is handled entirely here and nothing above this class knows about it. With
 * `security.protocol = SSL` the socket is connected first and the TLS handshake is performed on it afterwards
 * ({@see stream_socket_enable_crypto}), which is what a Kafka broker expects on its SSL listener: it speaks TLS from
 * the very first byte of the connection, there is no protocol-level upgrade.
 *
 * `SASL_PLAINTEXT` and `SASL_SSL` add an authentication exchange to that, right after the connection is opened and,
 * for `SASL_SSL`, right after the TLS handshake: one `SaslHandshake` request that names the mechanism, and then the
 * tokens of that mechanism - from Kafka 1.0 on inside `SaslAuthenticate` requests, which is what a **v1** handshake
 * asks for, and as bare size-prefixed frames after a v0 one ({@see SocketStream::authenticate()}). Only the PLAIN
 * mechanism is implemented, see {@see SaslMechanism}.
 *
 * @see docs/protocol/2.8.md, section "Transport security (SSL)"
 */
class SocketStream extends AbstractStream
{
    /**
     * Internal socket
     *
     * @var resource|null
     */
    protected $streamSocket;

    /**
     * Host name
     */
    protected string $host;

    /**
     * Port number
     */
    protected int $port;

    /**
     * Timeout for the connection, in seconds
     */
    protected float $timeout;

    /**
     * Flag that determines if the connection was established
     */
    protected bool $isConnected = false;

    /**
     * Transport this stream connects with, one of the implemented {@see SecurityProtocol} values
     */
    protected string $securityProtocol;

    /**
     * Mechanism the connection authenticates with, one of the implemented {@see SaslMechanism} values
     *
     * Only meaningful for a SASL transport; it stays at the configured value for the others, which never look at it.
     */
    protected string $saslMechanism;

    /**
     * Guard against a transparent reconnect while the authentication of a connection is still running
     *
     * The read and write loops re-open a connection that dropped before the first byte of a frame, which is exactly
     * what a broker does to reject credentials - without this flag the rejection would reconnect and authenticate
     * again, endlessly.
     */
    private bool $isAuthenticating = false;

    /**
     * Session lifetime the broker answered the last SaslAuthenticate with, null before an authentication
     *
     * The `session_lifetime_ms` of KIP-368 (SaslAuthenticate **v1**, Kafka 2.2): the number of milliseconds after
     * which the broker stops serving this connection unless the client has re-authenticated over it. **0** means
     * that the session never expires, which is what a broker without a `connections.max.reauth.ms` for this
     * listener answers - the container of this line among them. The re-authentication itself is not implemented
     * (it belongs to the SaslAuthenticate v2 of Kafka 2.5); the value is kept so that it can be.
     */
    private ?int $saslSessionLifetimeMs = null;

    /**
     * Socket stream constructor
     *
     * @param string               $tcpAddress        Tcp address for connection
     * @param array<string, mixed> $configuration     Configuration options
     * @param float|int|null       $connectionTimeout Timeout for the connection, in seconds
     */
    public function __construct(string $tcpAddress, protected array $configuration = [], $connectionTimeout = null)
    {
        $tcpInfo = parse_url($tcpAddress);
        if ($tcpInfo === false || !isset($tcpInfo['host'])) {
            throw new NetworkException(['error' => "Malformed tcp address: {$tcpAddress}"]);
        }
        $this->host = $tcpInfo['host'];
        $this->port = (int) ($tcpInfo['port'] ?? 9092);
        // ini_get() returns a string, whereas stream_socket_client() declares a float parameter
        $this->timeout          = (float) ($connectionTimeout ?? ini_get('default_socket_timeout'));
        $this->securityProtocol = self::resolveSecurityProtocol($configuration);
        $this->saslMechanism    = self::resolveSaslMechanism($this->securityProtocol, $configuration);
    }

    /**
     * Returns the transport this stream connects with
     */
    public function getSecurityProtocol(): string
    {
        return $this->securityProtocol;
    }

    /**
     * Returns the session lifetime of the last SASL authentication, or null when there was none
     *
     * See {@see self::$saslSessionLifetimeMs}: 0 is the answer of a broker that never expires the session, which
     * is every listener of the container this line is verified against.
     */
    public function getSaslSessionLifetimeMs(): ?int
    {
        return $this->saslSessionLifetimeMs;
    }

    /**
     * Returns the SASL mechanism this stream authenticates with, for a SASL transport
     */
    public function getSaslMechanism(): string
    {
        return $this->saslMechanism;
    }

    public function write(string $format, ...$arguments): void
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $packedData = pack($format, ...$arguments);
        $totalBytes = strlen($packedData);

        $isReconnected = false;
        for ($written = 0; $written < $totalBytes;) {
            $result = @fwrite($this->streamSocket, substr($packedData, $written));
            if ($result === false || $result === 0) {
                // Nothing has been sent yet, so a dropped connection can still be retried transparently
                if (!$isReconnected && !$this->isAuthenticating && $written === 0 && !$this->isConnected()) {
                    $this->connect();
                    $isReconnected = true;
                    continue;
                }

                throw new NetworkException(['error' => 'Can not write to the stream', 'written' => $written]);
            }
            $written += $result;
        }
    }

    public function read(string $format): array
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $packetSize = self::packetSize($format);
        $arguments  = unpack($format, $this->readExactly($packetSize));
        if ($arguments === false) {
            throw new \InvalidArgumentException("Can not unpack the data with the format: {$format}");
        }

        return $arguments;
    }

    /**
     * Automatic resource clean up
     */
    final public function __destruct()
    {
        $this->disconnect();
    }

    public function isConnected(): bool
    {
        return is_resource($this->streamSocket) && stream_socket_get_name($this->streamSocket, true) !== false;
    }

    public function isEmpty(): bool
    {
        return !is_resource($this->streamSocket) || feof($this->streamSocket);
    }

    /**
     * Returns the underlying socket resource, opening the connection first if it is not established yet.
     *
     * The client waits for the answers of several brokers at once with `stream_select()`, which needs the socket
     * resources themselves; every other operation goes through the methods of this class.
     *
     * @return resource
     */
    public function getStreamSocket()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->streamSocket;
    }

    /**
     * @return array<string, mixed>
     */
    public function __debugInfo(): array
    {
        return [
            'host' => $this->host,
            'port' => $this->port,
        ];
    }

    /**
     * Reads exactly the requested amount of bytes, looping until they have all arrived.
     *
     * A single fread() on a socket returns whatever is available at that moment, which is regularly less than a
     * whole protocol frame.
     */
    protected function readExactly(int $length): string
    {
        if ($length <= 0) {
            return '';
        }

        $buffer        = '';
        $isReconnected = false;
        while (($received = strlen($buffer)) < $length) {
            $chunk = @fread($this->streamSocket, $length - $received);
            if ($chunk === false || $chunk === '') {
                $metadata = is_resource($this->streamSocket) ? stream_get_meta_data($this->streamSocket) : [];
                if (!empty($metadata['timed_out'])) {
                    throw new NetworkException([
                        'error'    => 'Timeout while reading from the stream',
                        'expected' => $length,
                        'received' => $received,
                    ]);
                }
                // Only a connection that dropped before the first byte of a frame can be retried transparently,
                // reconnecting in the middle of a frame would resume the parser at an arbitrary offset
                if (!$isReconnected && !$this->isAuthenticating && $received === 0 && !$this->isConnected()) {
                    $this->connect();
                    $isReconnected = true;
                    continue;
                }

                throw new NetworkException([
                    'error'    => 'Unexpected end of stream',
                    'expected' => $length,
                    'received' => $received,
                ]);
            }
            $buffer .= $chunk;
        }

        return $buffer;
    }

    /**
     * Performs connection to the specified socket address.
     *
     * A stream connects itself lazily on the first read or write; this method is public so that the connection
     * layer can establish a connection up front, before it hands the socket to `stream_select()`.
     */
    public function connect(): void
    {
        $socketFlags = STREAM_CLIENT_CONNECT;
        if (!empty($this->configuration[ClientConfig::STREAM_ASYNC_CONNECT])) {
            $socketFlags |= STREAM_CLIENT_ASYNC_CONNECT;
        }
        if (!empty($this->configuration[ClientConfig::STREAM_PERSISTENT_CONNECTION])) {
            $socketFlags |= STREAM_CLIENT_PERSISTENT;
        }
        $streamSocket = @stream_socket_client(
            "tcp://{$this->host}:{$this->port}",
            $errorNumber,
            $errorString,
            $this->timeout,
            $socketFlags,
            $this->createStreamContext()
        );
        if ($streamSocket === false) {
            throw new NetworkException(['errorNumber' => $errorNumber, 'errorString' => $errorString]);
        }

        $sendBuffer    = $this->configuration[ClientConfig::SEND_BUFFER_BYTES] ?? null;
        $receiveBuffer = $this->configuration[ClientConfig::RECEIVE_BUFFER_BYTES] ?? null;
        if ($sendBuffer !== null) {
            stream_set_write_buffer($streamSocket, (int) $sendBuffer);
        }
        if ($receiveBuffer !== null) {
            stream_set_read_buffer($streamSocket, (int) $receiveBuffer);
        }

        // The connection timeout only covers the handshake, the read/write timeout is driven by request.timeout.ms
        $requestTimeoutMs = (int) ($this->configuration[ClientConfig::REQUEST_TIMEOUT_MS] ?? ($this->timeout * 1000));
        stream_set_timeout($streamSocket, intdiv($requestTimeoutMs, 1000), ($requestTimeoutMs % 1000) * 1000);

        // The TLS handshake happens before a single Kafka byte is exchanged, so it is bounded by the connection
        // timeout of this stream, not by `request.timeout.ms` - that is how the underlying OpenSSL loop of PHP
        // behaves, it keeps the deadline the socket was created with.
        if (SecurityProtocol::isEncrypted($this->securityProtocol)) {
            $this->encryptChannel($streamSocket);
        }

        $this->streamSocket = $streamSocket;
        $this->isConnected  = true;

        // The authentication is an exchange of ordinary frames, so it runs on the connected stream itself; a
        // failure leaves no usable connection behind, therefore the socket is dropped with it.
        if (SecurityProtocol::isSasl($this->securityProtocol)) {
            try {
                $this->authenticate();
            } catch (\Throwable $exception) {
                $this->disconnect();

                throw $exception;
            }
        }
    }

    /**
     * Performs the disconnect operation.
     *
     * The stream stays usable afterwards: the next read or write opens a fresh connection to the same address.
     *
     * @see \Protocol\Kafka\Network\ConnectionFactory::close()
     */
    public function disconnect(): void
    {
        if (is_resource($this->streamSocket)
            && empty($this->configuration[ClientConfig::STREAM_PERSISTENT_CONNECTION])
        ) {
            fclose($this->streamSocket);
        }
        $this->streamSocket = null;
        $this->isConnected  = false;
    }

    /**
     * Version of the `SaslHandshake` request this stream opens the authentication with.
     *
     * This is the one switch between the two SASL exchanges a Kafka 1.1.1 broker serves, and it is deliberately not
     * a `ClientConfig` option: a client has nothing to gain from the older one. Version **1** is what
     * {@see SaslHandshakeRequest} declares and therefore what is sent - the tokens are then framed as
     * {@see SaslAuthenticateRequest} and a refused credential comes back as the error code 58. Overriding this with
     * `SaslHandshakeRequestV0::VERSION` selects the raw, unframed exchange of the lines up to 0.11, which a 1.1.1
     * broker still serves unchanged; the tests use it to keep that path covered.
     *
     * @see \Protocol\Kafka\Tests\Unit\IO\SocketStreamSaslTest
     */
    protected function saslHandshakeVersion(): int
    {
        return SaslHandshakeRequest::VERSION;
    }

    /**
     * Authenticates the freshly opened connection with SASL, as `SaslServerAuthenticator` @ 1.1.1 expects it.
     *
     * The exchange opens with a Kafka request in both of its shapes: `SaslHandshake` (api key 17) names the
     * mechanism and is answered with an error code and the mechanisms the broker enabled. Its **version decides how
     * the tokens after it travel**, and the broker switches on nothing else:
     *
     * * **v1** (Kafka 1.0, KIP-152, the version this client sends): every token is the single field of a
     *   {@see SaslAuthenticateRequest}, an ordinary request with a header and a correlation id, and every answer is
     *   a {@see SaslAuthenticateResponse} with an error code, a message and the token of the broker. Wrong
     *   credentials are the error code **58** with a message instead of a silently dropped connection.
     * * **v0** (Kafka 0.10, KIP-43): the tokens are bare size-prefixed frames with no header at all, and a broker
     *   that refuses them closes the connection without an answer - there is no error code in that exchange.
     *
     * For PLAIN either shape is a single round trip: the client sends `\0<username>\0<password>` and the broker
     * answers with an empty token, after which the connection carries ordinary requests.
     *
     * @throws SaslAuthenticationException When the broker refused the credentials, however it said so
     * @throws KafkaException When the broker refused the mechanism (error code 33)
     */
    private function authenticate(): void
    {
        $clientId         = (string) ($this->configuration[ClientConfig::CLIENT_ID] ?? '');
        $correlationId    = AbstractRequest::nextCorrelationId();
        $handshakeVersion = $this->saslHandshakeVersion();

        $this->isAuthenticating = true;

        try {
            $handshake = $handshakeVersion >= SaslHandshakeRequest::VERSION
                ? new SaslHandshakeRequest($this->saslMechanism, $clientId, $correlationId)
                : new SaslHandshakeRequestV0($this->saslMechanism, $clientId, $correlationId);
            $handshake->writeTo($this);

            $response = ResponseValidator::read(
                SaslHandshakeResponse::class,
                $this,
                $correlationId,
                ['host' => $this->host, 'port' => $this->port]
            );

            if ($response->errorCode !== 0) {
                // 33 UnsupportedSaslMechanism is the code of a mechanism the broker has not enabled and 34
                // IllegalSaslState the one of a handshake that came at the wrong moment; the broker closes the
                // connection right after either of them, so there is nothing to recover on it
                throw KafkaException::fromCode($response->errorCode, [
                    'mechanism'         => $this->saslMechanism,
                    'enabledMechanisms' => $response->enabledMechanisms,
                    'host'              => $this->host,
                    'port'              => $this->port,
                ]);
            }

            if ($handshakeVersion >= SaslHandshakeRequest::VERSION) {
                $this->exchangeFramedPlainToken($clientId);
            } else {
                $this->exchangePlainToken();
            }
        } finally {
            $this->isAuthenticating = false;
        }
    }

    /**
     * Performs the token exchange of the PLAIN mechanism inside `SaslAuthenticate` requests (SaslHandshake v1)
     *
     * @param string $clientId Client id of the connection, carried by the header of the request
     *
     * @throws SaslAuthenticationException When the broker refused the credentials or answered something else
     */
    private function exchangeFramedPlainToken(string $clientId): void
    {
        $username      = (string) $this->configuration[ClientConfig::SASL_USERNAME];
        $password      = (string) $this->configuration[ClientConfig::SASL_PASSWORD];
        $correlationId = AbstractRequest::nextCorrelationId();

        $token = SaslToken::ofPlainCredentials($username, $password);
        new SaslAuthenticateRequest($token->token, $clientId, $correlationId)->writeTo($this);

        try {
            $answer = ResponseValidator::read(
                SaslAuthenticateResponse::class,
                $this,
                $correlationId,
                ['host' => $this->host, 'port' => $this->port]
            );
        } catch (NetworkException $exception) {
            // A 1.1.1 broker answers this request even when it refuses the credentials, so a closed connection here
            // means something else entirely - a v1 handshake it never got, or a frame it could not parse
            throw new SaslAuthenticationException(
                [
                    'error'     => 'The broker closed the connection instead of answering the SaslAuthenticate '
                        . 'request of the SASL token exchange',
                    'mechanism' => $this->saslMechanism,
                    'username'  => $username,
                    'host'      => $this->host,
                    'port'      => $this->port,
                ],
                $exception
            );
        }

        $this->saslSessionLifetimeMs = $answer->sessionLifetimeMs;

        if ($answer->errorCode !== 0) {
            // 58 SaslAuthenticationFailed is what wrong credentials look like from Kafka 1.0 on, and the broker
            // closes the connection right after this answer; 34 IllegalSaslState is a request at the wrong moment
            throw new SaslAuthenticationException(
                [
                    'error'        => (string) ($answer->errorMessage ?? 'the broker refused the credentials'),
                    'errorCode'    => $answer->errorCode,
                    'errorMessage' => $answer->errorMessage,
                    'mechanism'    => $this->saslMechanism,
                    'username'     => $username,
                    'host'         => $this->host,
                    'port'         => $this->port,
                ],
                KafkaException::fromCode($answer->errorCode, [
                    'errorMessage' => $answer->errorMessage,
                    'mechanism'    => $this->saslMechanism,
                ])
            );
        }

        // PLAIN has exactly one round trip: `PlainSaslServer.evaluateResponse()` answers with `new byte[0]`, which
        // the broker sends back as the empty `sasl_auth_bytes` of the response. Anything else would mean that the
        // mechanism expects another token, and PLAIN never does.
        if ($answer->saslAuthBytes !== null && $answer->saslAuthBytes !== '') {
            throw new SaslAuthenticationException([
                'error'     => 'The broker answered the PLAIN token with a non-empty token, which the mechanism '
                    . 'does not define',
                'mechanism' => $this->saslMechanism,
                'answer'    => bin2hex($answer->saslAuthBytes),
                'host'      => $this->host,
                'port'      => $this->port,
            ]);
        }
    }

    /**
     * Performs the raw token exchange of the PLAIN mechanism on a connection handshaken with v0
     *
     * @throws SaslAuthenticationException When the broker rejected the credentials by closing the connection
     */
    private function exchangePlainToken(): void
    {
        $username = (string) $this->configuration[ClientConfig::SASL_USERNAME];
        $password = (string) $this->configuration[ClientConfig::SASL_PASSWORD];

        SaslToken::ofPlainCredentials($username, $password)->writeTo($this);

        try {
            $answer = SaslToken::readFrom($this);
        } catch (NetworkException $exception) {
            throw new SaslAuthenticationException(
                [
                    'error'     => 'The broker closed the connection during the SASL token exchange, which is how a '
                        . 'broker rejects credentials after a SaslHandshake v0 - the error code 58 of Kafka 1.0 '
                        . 'needs the framed SaslAuthenticate exchange of a v1 handshake',
                    'mechanism' => $this->saslMechanism,
                    'username'  => $username,
                    'host'      => $this->host,
                    'port'      => $this->port,
                ],
                $exception
            );
        }

        // PLAIN has exactly one round trip: `PlainSaslServer.evaluateResponse()` answers with `new byte[0]` and
        // marks the exchange complete. Anything else would mean that the broker expects another token.
        if (!$answer->isEmpty()) {
            throw new SaslAuthenticationException([
                'error'     => 'The broker answered the PLAIN token with a non-empty token, which the mechanism '
                    . 'does not define',
                'mechanism' => $this->saslMechanism,
                'answer'    => bin2hex((string) $answer->token),
                'host'      => $this->host,
                'port'      => $this->port,
            ]);
        }
    }

    /**
     * Validates the configured `security.protocol` and returns it
     *
     * @param array<string, mixed> $configuration Configuration options
     */
    private static function resolveSecurityProtocol(array $configuration): string
    {
        $securityProtocol = $configuration[ClientConfig::SECURITY_PROTOCOL] ?? SecurityProtocol::PLAINTEXT;
        if (!is_string($securityProtocol)) {
            $securityProtocol = get_debug_type($securityProtocol);
        }
        if (in_array($securityProtocol, SecurityProtocol::implemented(), true)) {
            return $securityProtocol;
        }

        $known = implode(', ', SecurityProtocol::all());

        throw new InvalidConfigurationException(
            "Unknown security protocol {$securityProtocol}, expected one of: {$known}."
        );
    }

    /**
     * Validates the configured `sasl.mechanism` and its credentials, and returns the mechanism
     *
     * Nothing is validated for a transport that does not authenticate: a `sasl.*` option next to
     * `security.protocol = PLAINTEXT` is as meaningless as an `ssl.*` one and is ignored the same way.
     *
     * @param string               $securityProtocol Transport the stream connects with
     * @param array<string, mixed> $configuration    Configuration options
     */
    private static function resolveSaslMechanism(string $securityProtocol, array $configuration): string
    {
        $mechanism = $configuration[ClientConfig::SASL_MECHANISM] ?? SaslMechanism::PLAIN;
        if (!is_string($mechanism)) {
            $mechanism = get_debug_type($mechanism);
        }
        if (!SecurityProtocol::isSasl($securityProtocol)) {
            return $mechanism;
        }

        if (!in_array($mechanism, SaslMechanism::implemented(), true)) {
            throw new InvalidConfigurationException(self::unsupportedMechanismMessage($mechanism));
        }

        foreach ([ClientConfig::SASL_USERNAME, ClientConfig::SASL_PASSWORD] as $option) {
            $value = $configuration[$option] ?? null;
            if (!is_string($value) || $value === '') {
                throw new InvalidConfigurationException(
                    "The SASL mechanism {$mechanism} needs a non-empty {$option}: the broker splits the token into "
                    . 'the authorization id, the user name and the password, and refuses an empty one.'
                );
            }
        }

        return $mechanism;
    }

    /**
     * Explains why a mechanism that a Kafka 1.1.1 broker knows is not available in this client
     */
    private static function unsupportedMechanismMessage(string $mechanism): string
    {
        $implemented = implode(', ', SaslMechanism::implemented());

        $reason = match ($mechanism) {
            SaslMechanism::GSSAPI => 'GSSAPI is Kerberos, and PHP has no GSS-API binding in core to produce the '
                . 'tokens a broker would accept',
            SaslMechanism::SCRAM_SHA_256, SaslMechanism::SCRAM_SHA_512 => 'the SCRAM mechanisms of Kafka 0.10.2 '
                . '(KIP-84) need the multi-round exchange of RFC 5802, which this client does not perform',
            default => 'a Kafka 1.1.1 broker only knows ' . implode(', ', SaslMechanism::all()),
        };

        return "The SASL mechanism {$mechanism} is not implemented: {$reason}. Use {$implemented}.";
    }

    /**
     * Creates the context for the underlying socket from the configuration
     *
     * Nothing but the TLS material ever goes into the context, so a plaintext connection is opened with an empty
     * one; that keeps a single code path for both transports.
     *
     * @return resource
     */
    private function createStreamContext()
    {
        if (!SecurityProtocol::isEncrypted($this->securityProtocol)) {
            return stream_context_create();
        }

        $contextOptions = [
            'ssl' => [
                // The certificate of the broker is verified, and its subject has to match the host it was reached
                // at. A self-signed broker certificate therefore needs `ssl.ca.cert.location` pointing at it.
                'verify_peer'       => true,
                'verify_peer_name'  => true,
                'peer_name'         => $this->host,
                'allow_self_signed' => false,
                'crypto_method'     => $this->cryptoMethod(),
            ],
        ];

        if (!empty($this->configuration[ClientConfig::SSL_CA_CERT_LOCATION])) {
            $contextOptions['ssl']['cafile'] = $this->ensureValidFile(
                (string) $this->configuration[ClientConfig::SSL_CA_CERT_LOCATION],
                'CA file {file} is not accessible.'
            );
        }

        if (!empty($this->configuration[ClientConfig::SSL_CLIENT_CERT_LOCATION])) {
            $contextOptions['ssl']['local_cert'] = $this->ensureValidFile(
                (string) $this->configuration[ClientConfig::SSL_CLIENT_CERT_LOCATION],
                'Client certificate file {file} is not accessible.'
            );
        }

        if (!empty($this->configuration[ClientConfig::SSL_KEY_LOCATION])) {
            $contextOptions['ssl']['local_pk'] = $this->ensureValidFile(
                (string) $this->configuration[ClientConfig::SSL_KEY_LOCATION],
                'Key file {file} is not accessible.'
            );
        }

        if (!empty($this->configuration[ClientConfig::SSL_KEY_PASSWORD])) {
            $contextOptions['ssl']['passphrase'] = (string) $this->configuration[ClientConfig::SSL_KEY_PASSWORD];
        }

        return stream_context_create($contextOptions);
    }

    /**
     * Validates the given file name and returns it as a result
     *
     * @param string $fileName     File name to validate
     * @param string $errorMessage Message to show if the file is not accessible
     *
     * @return string Given file name
     */
    private function ensureValidFile(string $fileName, string $errorMessage): string
    {
        if (!is_readable($fileName)) {
            throw new InvalidConfigurationException(strtr($errorMessage, ['{file}' => $fileName]));
        }

        return $fileName;
    }

    /**
     * Translates `ssl.enabled.protocols` / `ssl.protocol` into the crypto method bitmask of PHP
     *
     * `ssl.enabled.protocols` wins when it is set, exactly like in the Java client, where `ssl.protocol` only picks
     * the SSLContext and the enabled protocols narrow it down afterwards.
     */
    private function cryptoMethod(): int
    {
        static $cipherMap = [
            SslProtocol::TLS     => STREAM_CRYPTO_METHOD_TLS_CLIENT,
            SslProtocol::TLSv1_1 => STREAM_CRYPTO_METHOD_TLSv1_1_CLIENT,
            SslProtocol::TLSv1_2 => STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT,
            SslProtocol::SSL     => STREAM_CRYPTO_METHOD_SSLv23_CLIENT,
            SslProtocol::SSLv2   => STREAM_CRYPTO_METHOD_SSLv2_CLIENT,
            SslProtocol::SSLv3   => STREAM_CRYPTO_METHOD_SSLv3_CLIENT,
        ];

        $enabledProtocols = $this->configuration[ClientConfig::SSL_ENABLED_PROTOCOLS] ?? null;
        if (empty($enabledProtocols)) {
            $enabledProtocols = [$this->configuration[ClientConfig::SSL_PROTOCOL] ?? SslProtocol::TLS];
        }
        if (!is_array($enabledProtocols)) {
            $enabledProtocols = array_map(trim(...), explode(',', (string) $enabledProtocols));
        }

        $cryptoMethod = 0;
        foreach ($enabledProtocols as $sslProtocol) {
            if (!is_string($sslProtocol) || !isset($cipherMap[$sslProtocol])) {
                $name = is_string($sslProtocol) ? $sslProtocol : get_debug_type($sslProtocol);

                throw new InvalidConfigurationException("SSL protocol {$name} is not implemented.");
            }
            $cryptoMethod |= $cipherMap[$sslProtocol];
        }

        return $cryptoMethod;
    }

    /**
     * Encrypts the channel between the client and the broker
     *
     * @param resource $streamSocket Underlying socket
     */
    private function encryptChannel($streamSocket): void
    {
        $cryptoMethod = $this->cryptoMethod();

        $errorMessage = null;
        set_error_handler(static function (int $code, string $message) use (&$errorMessage): bool {
            $errorMessage = trim(str_replace('stream_socket_enable_crypto():', '', $message));

            return true;
        });

        try {
            $isCryptoEnabled = stream_socket_enable_crypto($streamSocket, true, $cryptoMethod);
        } finally {
            restore_error_handler();
        }

        // 0 means "not enough data yet", which can only happen on a non-blocking socket; this client never
        // hands one to the handshake, so it is treated as a failure rather than looped over
        if ($isCryptoEnabled !== true) {
            fclose($streamSocket);

            throw new NetworkException([
                'error' => 'Failed to enable encryption of the connection'
                    . ($errorMessage !== null && $errorMessage !== '' ? ": {$errorMessage}" : '.'),
                'host'  => $this->host,
                'port'  => $this->port,
            ]);
        }
    }
}
