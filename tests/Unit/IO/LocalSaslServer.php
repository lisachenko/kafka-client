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

use RuntimeException;

/**
 * A scripted SASL listener that runs in a child PHP process, used to exercise the authentication exchange of
 * {@see \Protocol\Kafka\IO\SocketStream} without a broker.
 *
 * The exchange is a conversation - the client writes a frame and blocks until the answer arrives - so the peer
 * cannot live in the same process as the client that drives it, exactly as for {@see LocalTlsServer}. The child
 * reads the frames the client sends, records them, and answers with the frames of the scenario it was started
 * with, which is how every path a real broker can take is reproduced here: the accepted handshake, the refused
 * mechanism, and the connection that is dropped instead of an answer.
 *
 * The child speaks **both** exchanges of a Kafka 1.1.1 broker and decides between them the way
 * `SaslServerAuthenticator.handleHandshakeRequest()` @ 1.1.1 does - on the `ApiVersion` of the handshake alone: a
 * v1 handshake is followed by `SaslAuthenticate` requests with a header and an error code, a v0 one by bare
 * size-prefixed token frames.
 *
 * @see \Protocol\Kafka\Tests\Integration\SaslTransportTest for the same exchange against a real broker
 */
final class LocalSaslServer
{
    /**
     * The broker accepts PLAIN and answers the token with the empty token of a completed exchange
     */
    public const string SCENARIO_ACCEPT = 'accept';

    /**
     * The broker has another mechanism enabled: error code 33 and the list of the enabled ones, then it closes
     */
    public const string SCENARIO_UNSUPPORTED_MECHANISM = 'unsupported-mechanism';

    /**
     * The handshake itself is refused with the error code 34 and an empty mechanism list, then the broker closes
     */
    public const string SCENARIO_ILLEGAL_SASL_STATE = 'illegal-sasl-state';

    /**
     * The credentials are refused: the connection is closed during the token exchange, without an answer
     */
    public const string SCENARIO_REFUSE_CREDENTIALS = 'refuse-credentials';

    /**
     * The credentials are refused with the error code 58 and a message, the answer of a v1 exchange
     */
    public const string SCENARIO_INVALID_CREDENTIALS = 'invalid-credentials';

    /**
     * The broker answers the token with a token of its own, which the PLAIN mechanism does not define
     */
    public const string SCENARIO_UNEXPECTED_TOKEN = 'unexpected-token';

    /**
     * The handshake is answered with the correlation id of another request
     */
    public const string SCENARIO_WRONG_CORRELATION_ID = 'wrong-correlation-id';

    /**
     * The message a 1.1.1 broker answers a refused PLAIN credential with
     */
    public const string INVALID_CREDENTIALS_MESSAGE = 'Authentication failed: Invalid username or password';

    /**
     * How long the child waits for a connection before it gives up, in seconds
     */
    private const int ACCEPT_TIMEOUT = 10;

    /**
     * The child process running the server
     *
     * @var resource|null
     */
    private $process;

    /**
     * Pipes of the child process
     *
     * @var array<int, resource>
     */
    private array $pipes = [];

    /**
     * The `host:port` the server listens on
     */
    private string $address = '';

    /**
     * File the child appends the hex of every received frame to, one per line
     */
    private string $captureFile;

    /**
     * Starts a listener that plays the given scenario for exactly one connection
     */
    public function __construct(string $scenario)
    {
        $this->captureFile = (string) tempnam(sys_get_temp_dir(), 'kafka-sasl-');

        $descriptors = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $process     = proc_open(
            [PHP_BINARY, '-r', self::childScript(), $scenario, $this->captureFile],
            $descriptors,
            $this->pipes
        );
        if (!is_resource($process)) {
            throw new RuntimeException('Can not start the local SASL server');
        }
        $this->process = $process;

        stream_set_timeout($this->pipes[1], self::ACCEPT_TIMEOUT);
        $address = fgets($this->pipes[1]);
        if ($address === false || trim($address) === '') {
            throw new RuntimeException('The local SASL server did not report its address: ' . $this->errorOutput());
        }
        $this->address = trim($address);
    }

    /**
     * Returns the `host:port` the server listens on
     */
    public function address(): string
    {
        return $this->address;
    }

    /**
     * Returns the frames the client sent, as hex strings, in the order they arrived
     *
     * @return list<string>
     */
    public function receivedFrames(): array
    {
        $content = (string) file_get_contents($this->captureFile);

        return array_values(array_filter(array_map(trim(...), explode("\n", $content))));
    }

    /**
     * Everything the child process wrote to its standard error stream
     */
    public function errorOutput(): string
    {
        if (!isset($this->pipes[2])) {
            return '';
        }
        stream_set_blocking($this->pipes[2], false);

        return (string) stream_get_contents($this->pipes[2]);
    }

    /**
     * Stops the server and removes its capture file
     */
    public function stop(): void
    {
        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];
        if (is_resource($this->process)) {
            proc_terminate($this->process);
            proc_close($this->process);
        }
        $this->process = null;
        if (is_file($this->captureFile)) {
            unlink($this->captureFile);
        }
    }

    /**
     * The program of the child process: announce the address, accept one connection and play the scenario on it
     *
     * The frames are built by hand here on purpose - a scripted peer that used the classes under test could not
     * prove that they produce the bytes of the protocol.
     */
    private static function childScript(): string
    {
        return <<<'PHP'
            [$scenario, $captureFile] = [$argv[1], $argv[2]];

            $server = @stream_socket_server('tcp://127.0.0.1:0', $errorNumber, $errorString);
            if ($server === false) {
                fwrite(STDERR, "can not listen: {$errorString}\n");
                exit(1);
            }
            echo stream_socket_get_name($server, false), "\n";

            $connection = @stream_socket_accept($server, 10);
            if ($connection === false) {
                fwrite(STDERR, "nobody connected\n");
                exit(1);
            }

            $readFrame = static function () use ($connection, $captureFile): ?string {
                $header = '';
                while (strlen($header) < 4) {
                    $chunk = fread($connection, 4 - strlen($header));
                    if ($chunk === false || $chunk === '') {
                        return null;
                    }
                    $header .= $chunk;
                }
                $size = unpack('Nsize', $header)['size'];
                $body = '';
                while (strlen($body) < $size) {
                    $chunk = fread($connection, $size - strlen($body));
                    if ($chunk === false || $chunk === '') {
                        return null;
                    }
                    $body .= $chunk;
                }
                file_put_contents($captureFile, bin2hex($header . $body) . "\n", FILE_APPEND);

                return $header . $body;
            };
            $answer = static function (string $hex) use ($connection): void {
                fwrite($connection, (string) hex2bin($hex));
            };

            $handshake = $readFrame();
            if ($handshake === null) {
                exit(0);
            }
            // The version of the handshake decides how the tokens after it are framed, and the correlation id the
            // client generated is echoed back in the response header
            $handshakeVersion = unpack('nversion', substr($handshake, 6, 2))['version'];
            $correlationId    = bin2hex(substr($handshake, 8, 4));

            if ($scenario === 'unsupported-mechanism') {
                // ErrorCode 33, EnabledMechanisms = ["GSSAPI"]
                $answer('00000012' . $correlationId . '0021' . '00000001' . '0006' . bin2hex('GSSAPI'));
                fclose($connection);
                exit(0);
            }
            if ($scenario === 'illegal-sasl-state') {
                // ErrorCode 34 with an empty mechanism list, the answer of KafkaApis @ 1.1.1
                $answer('0000000a' . $correlationId . '0022' . '00000000');
                fclose($connection);
                exit(0);
            }
            if ($scenario === 'wrong-correlation-id') {
                $answer('00000011' . '7fffffff' . '0000' . '00000001' . '0005' . bin2hex('PLAIN'));
                fclose($connection);
                exit(0);
            }

            // ErrorCode 0, EnabledMechanisms = ["PLAIN"]
            $answer('00000011' . $correlationId . '0000' . '00000001' . '0005' . bin2hex('PLAIN'));

            $token = $readFrame();
            if ($token === null) {
                exit(0);
            }
            if ($scenario === 'refuse-credentials') {
                fclose($connection);
                exit(0);
            }

            if ($handshakeVersion >= 1) {
                // The token arrived as a SaslAuthenticate request; the answer carries its correlation id, an error
                // code, a nullable error message, the token of the broker and - from the version 1 of KIP-368 that
                // the client sends since Kafka 2.2 - the session lifetime, which this broker never bounds
                $tokenCorrelationId = bin2hex(substr($token, 8, 4));
                $tokenVersion       = unpack('nversion', substr($token, 6, 2))['version'];
                $sessionLifetime    = $tokenVersion >= 1 ? '0000000000000000' : '';
                if ($scenario === 'invalid-credentials') {
                    // ErrorCode 58 (0x003a), the message of the broker, and the empty token
                    $message = 'Authentication failed: Invalid username or password';
                    $body    = $tokenCorrelationId . '003a' . sprintf('%04x', strlen($message))
                        . bin2hex($message) . '00000000' . $sessionLifetime;
                    $answer(sprintf('%08x', strlen($body) / 2) . $body);
                    fclose($connection);
                    exit(0);
                }
                $body = $tokenCorrelationId . '0000' . 'ffff'
                    . ($scenario === 'unexpected-token' ? '00000004' . bin2hex('more') : '00000000')
                    . $sessionLifetime;
                $answer(sprintf('%08x', strlen($body) / 2) . $body);
            } elseif ($scenario === 'unexpected-token') {
                $answer('00000004' . bin2hex('more'));
            } else {
                // The empty token of a completed PLAIN exchange
                $answer('00000000');
            }

            // Whatever the client sends afterwards is an ordinary request, echoed back so that the test can see
            // that the connection stayed usable
            while (!feof($connection)) {
                $data = fread($connection, 8192);
                if ($data === false || $data === '') {
                    break;
                }
                fwrite($connection, $data);
            }
            PHP;
    }
}
