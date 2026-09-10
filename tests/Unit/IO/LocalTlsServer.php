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

/**
 * A TLS echo server that runs in a child PHP process, used to exercise the crypto of {@see \Protocol\Kafka\IO\SocketStream}.
 *
 * The handshake of a blocking socket cannot be driven from the same process as the client that performs it: the
 * client blocks inside `stream_socket_enable_crypto()` until the server answers, so the server has to live
 * somewhere else. A child process is the portable way to arrange that - `pcntl_fork()` is not available on every
 * build and would fork the test runner itself.
 *
 * The server presents the checked-in broker certificate of `docker/kafka-1.1.1/ssl`, so a client that trusts that
 * certificate sees exactly the handshake it performs against the SSL listener of the test broker.
 */
final class LocalTlsServer
{
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
     * The PEM file the child reads its certificate and key from
     */
    private string $certificateFile;

    /**
     * Starts a TLS echo server presenting the given certificate
     *
     * @param string $certificateFile Certificate in PEM format
     * @param string $keyFile         Private key of that certificate, in PEM format
     */
    public function __construct(string $certificateFile, string $keyFile)
    {
        $this->certificateFile = (string) tempnam(sys_get_temp_dir(), 'kafka-tls-');
        file_put_contents(
            $this->certificateFile,
            (string) file_get_contents($certificateFile) . (string) file_get_contents($keyFile)
        );
        chmod($this->certificateFile, 0o600);

        $descriptors = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $process     = proc_open(
            [PHP_BINARY, '-r', self::childScript(), $this->certificateFile],
            $descriptors,
            $this->pipes
        );
        if (!is_resource($process)) {
            throw new \RuntimeException('Can not start the local TLS server');
        }
        $this->process = $process;

        stream_set_timeout($this->pipes[1], self::ACCEPT_TIMEOUT);
        $address = fgets($this->pipes[1]);
        if ($address === false || trim($address) === '') {
            throw new \RuntimeException('The local TLS server did not report its address: ' . $this->errorOutput());
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
     * Stops the server and removes its temporary certificate
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
        if (is_file($this->certificateFile)) {
            unlink($this->certificateFile);
        }
    }

    /**
     * The program of the child process: announce the address, accept one TLS connection and echo it back
     */
    private static function childScript(): string
    {
        return <<<'PHP'
            $context = stream_context_create(['ssl' => ['local_cert' => $argv[1]]]);
            $server  = @stream_socket_server(
                'tls://127.0.0.1:0',
                $errorNumber,
                $errorString,
                STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
                $context
            );
            if ($server === false) {
                fwrite(STDERR, "can not listen: {$errorString}\n");
                exit(1);
            }
            echo stream_socket_get_name($server, false), "\n";
            $connection = @stream_socket_accept($server, 10);
            if ($connection === false) {
                fwrite(STDERR, "handshake failed\n");
                exit(1);
            }
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
