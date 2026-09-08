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
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Common\Security\SslProtocol;

/**
 * Implementation of the binary stream on top of a TCP socket, optionally wrapped in TLS.
 *
 * Kafka 0.9.0.0 introduced listeners per security protocol: the very same request bytes are exchanged whether the
 * connection is plain or encrypted, so the transport is handled entirely here and nothing above this class knows
 * about it. With `security.protocol = SSL` the socket is connected first and the TLS handshake is performed on it
 * afterwards ({@see stream_socket_enable_crypto}), which is what a Kafka broker expects on its SSL listener: it
 * speaks TLS from the very first byte of the connection, there is no protocol-level upgrade.
 *
 * SASL is not implemented; see {@see SecurityProtocol} for why 0.9 cannot support it.
 *
 * @see docs/protocol/0.9.0.md, section "Transport security (SSL)"
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
    }

    /**
     * Returns the transport this stream connects with
     */
    public function getSecurityProtocol(): string
    {
        return $this->securityProtocol;
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
                if (!$isReconnected && $written === 0 && !$this->isConnected()) {
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
                if (!$isReconnected && $received === 0 && !$this->isConnected()) {
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
        if ($this->securityProtocol === SecurityProtocol::SSL) {
            $this->encryptChannel($streamSocket);
        }

        $this->streamSocket = $streamSocket;
        $this->isConnected  = true;
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

        if (in_array($securityProtocol, SecurityProtocol::all(), true)) {
            throw new InvalidConfigurationException(
                "The security protocol {$securityProtocol} is not supported for Kafka 0.9: SASL is Kerberos-only "
                . 'there and is negotiated outside the Kafka protocol, the SaslHandshake request only exists from '
                . 'Kafka 0.10.0 on. Use ' . SecurityProtocol::PLAINTEXT . ' or ' . SecurityProtocol::SSL . '.'
            );
        }

        $known = implode(', ', SecurityProtocol::all());

        throw new InvalidConfigurationException(
            "Unknown security protocol {$securityProtocol}, expected one of: {$known}."
        );
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
        if ($this->securityProtocol !== SecurityProtocol::SSL) {
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
