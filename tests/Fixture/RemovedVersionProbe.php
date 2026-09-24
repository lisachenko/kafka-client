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

namespace Protocol\Kafka\Tests\Fixture;

use Protocol\Kafka\Protocol\Request\AbstractRequest;
use RuntimeException;

/**
 * Sends the frame of a request whose version Kafka 4.0 removed (KIP-896) and tells what the node did with it.
 *
 * A node of Kafka 4.0 or later does not answer a version below the minimum of an api with the error code 35: the
 * request parser throws `UnsupportedVersionException: Received request for api with key 0 (Produce) and unsupported
 * version 2`, `SocketServer`'s processor catches it and **closes the connection**, and `docker logs kafka-4-3-1`
 * says `ERROR Closing socket for ... because of error (kafka.network.Processor)` with that exception behind it. That
 * holds for Produce v0 to v2 as well, which the ApiVersions answer still lists (KAFKA-18659). The classes of those
 * versions stay in this package - for the wire vectors of the lines below and for a peer of Kafka 3.x - and this
 * probe is how the integration suite measures the refusal instead of sending them as if they were served.
 *
 * Every frame goes out on a connection of its own, so that one refusal can not cost the requests of a test.
 *
 * @see docs/protocol/4.3.md, section "The versions Kafka 4.0 removed (KIP-896)"
 */
final class RemovedVersionProbe
{
    /**
     * The node closed the connection without writing a byte
     */
    public const string CLOSED = 'closed';

    /**
     * A response frame came back
     */
    public const string ANSWERED = 'answered';

    /**
     * Nothing came back within the timeout and the connection stayed open
     */
    public const string SILENT = 'silent';

    /**
     * @param string $address Broker address as `host:port`
     */
    public function __construct(private readonly string $address, private readonly float $timeout = 5.0) {}

    /**
     * Sends the request on a fresh plain connection and returns {@see self::CLOSED}, {@see self::ANSWERED} or
     * {@see self::SILENT}
     */
    public function send(AbstractRequest $request): string
    {
        $socket = @stream_socket_client('tcp://' . $this->address, $errorNumber, $errorString, $this->timeout);
        if ($socket === false) {
            throw new RuntimeException("Can not connect to {$this->address}: {$errorString} ({$errorNumber})");
        }

        try {
            stream_set_timeout($socket, (int) ceil($this->timeout));
            fwrite($socket, (string) $request);

            $size = fread($socket, 4);
            if ($size !== false && $size !== '') {
                return self::ANSWERED;
            }

            return stream_get_meta_data($socket)['timed_out'] ? self::SILENT : self::CLOSED;
        } finally {
            fclose($socket);
        }
    }
}
