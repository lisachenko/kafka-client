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

use Protocol\Kafka\Common\Errors\NetworkException;

/**
 * In-memory implementation of the binary stream, used for framing and in the unit tests
 */
class StringStream extends AbstractStream
{
    /**
     * Internal binary buffer
     */
    private string $buffer;

    /**
     * String stream constructor.
     *
     * @param string|null $stringBuffer Optional buffer to read from
     */
    public function __construct(?string $stringBuffer = null)
    {
        $this->buffer = $stringBuffer ?? '';
    }

    public function write(string $format, ...$arguments): void
    {
        $this->buffer .= pack($format, ...$arguments);
    }

    public function read(string $format): array
    {
        $packetSize = self::packetSize($format);
        $available  = strlen($this->buffer);
        if ($available < $packetSize) {
            throw new NetworkException(
                ['error' => "Not enough data in the buffer: {$packetSize} bytes requested, {$available} available"]
            );
        }

        $arguments = unpack($format, $this->buffer);
        if ($arguments === false) {
            throw new \InvalidArgumentException("Can not unpack the data with the format: {$format}");
        }
        $this->buffer = substr($this->buffer, $packetSize);

        return $arguments;
    }

    public function isConnected(): bool
    {
        return true;
    }

    public function isEmpty(): bool
    {
        return $this->buffer === '';
    }

    /**
     * Returns the current buffer, useful for write operations
     */
    public function getBuffer(): string
    {
        return $this->buffer;
    }

    /**
     * Returns the number of bytes that are still available for reading
     */
    public function remaining(): int
    {
        return strlen($this->buffer);
    }
}
