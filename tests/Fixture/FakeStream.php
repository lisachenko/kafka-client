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

use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\AbstractStream;

/**
 * Stream double with separate read and write sides, so a canned server response can be replayed without a socket.
 *
 * A StringStream can not be used for this: it reads back whatever was written into it.
 */
final class FakeStream extends AbstractStream
{
    private string $readBuffer;

    private string $writtenBytes = '';

    public function __construct(string $readBuffer = '')
    {
        $this->readBuffer = $readBuffer;
    }

    public function write(string $format, ...$arguments): void
    {
        $this->writtenBytes .= pack($format, ...$arguments);
    }

    public function read(string $format): array
    {
        $packetSize = self::packetSize($format);
        $available  = strlen($this->readBuffer);
        if ($available < $packetSize) {
            throw new NetworkException(
                ['error' => "Not enough data in the buffer: {$packetSize} bytes requested, {$available} available"]
            );
        }

        $arguments        = unpack($format, $this->readBuffer);
        $this->readBuffer = substr($this->readBuffer, $packetSize);

        if ($arguments === false) {
            throw new \InvalidArgumentException("Can not unpack the data with the format: {$format}");
        }

        return $arguments;
    }

    public function isConnected(): bool
    {
        return true;
    }

    public function isEmpty(): bool
    {
        return $this->readBuffer === '';
    }

    /**
     * Returns everything that was written into this stream
     */
    public function getWrittenBytes(): string
    {
        return $this->writtenBytes;
    }
}
