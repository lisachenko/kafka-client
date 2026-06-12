<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\IO;

class StringStream extends AbstractStream
{
    /**
     * Internal binary buffer
     */
    private string $buffer;

    /**
     * String stream constructor.
     *
     * @param string $stringBuffer Optional buffer to read from
     */
    public function __construct(?string $stringBuffer = null)
    {
        $this->buffer = $stringBuffer ?? '';
    }

    /**
     * Writes arguments to the stream
     *
     * @param string $format       Format for packing arguments
     * @param array  ...$arguments List of arguments for packing
     *
     * @return void
     * @see pack() manual for format
     *
     */
    public function write(string $format, ...$arguments): void
    {
        $this->buffer .= pack($format, ...$arguments);
    }

    /**
     * Reads information from the stream, advanced internal pointer
     *
     * @param string $format Format for unpacking arguments
     *
     * @return array List of unpacked arguments
     * @see unpack() manual for format
     *
     */
    public function read(string $format): array
    {
        $arguments    = unpack($format, $this->buffer);
        $this->buffer = substr($this->buffer, self::packetSize($format));

        return $arguments;
    }

    /**
     * {@inheritdoc}
     */
    public function isConnected(): bool
    {
        return true;
    }

    /**
     * Returns the current buffer, useful for write operations
     */
    public function getBuffer(): string
    {
        return $this->buffer;
    }

    /**
     * Checks if stream is empty
     */
    public function isEmpty(): bool
    {
        return $this->buffer === '';
    }
}
