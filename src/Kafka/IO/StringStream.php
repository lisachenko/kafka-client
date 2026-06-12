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
     * String stream constructor.
     *
     * @param string $buffer Optional buffer to write to or read from
     */
    public function __construct(
        /**
         * Internal binary buffer
         */
        private $buffer = null
    ) {}

    /**
     * Writes arguments to the stream
     *
     * @param string $format       Format for packing arguments
     * @param array  ...$arguments List of arguments for packing
     *
     * @see pack() manual for format
     *
     * @return void
     */
    public function write($format, ...$arguments): void
    {
        $this->buffer .= pack($format, ...$arguments);
    }

    /**
     * Reads information from the stream, advanced internal pointer
     *
     * @param string $format Format for unpacking arguments
     * @see unpack() manual for format
     *
     * @return array List of unpacked arguments
     */
    public function read($format): array|false
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
     * Returns the current buffer, useful for write opertaions
     *
     * @return string
     */
    public function getBuffer()
    {
        return $this->buffer;
    }


    /**
     * Checks if stream is empty
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return (string) $this->buffer === '';
    }
}
