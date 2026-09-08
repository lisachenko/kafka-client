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
 * In-memory implementation of the binary stream, used for framing and for unit tests
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
     * @param string $stringBuffer Buffer to write to or read from, passed by reference to observe the written data
     */
    public function __construct(string &$stringBuffer = '')
    {
        $this->buffer = &$stringBuffer;
    }

    /**
     * Creates a stream over the copy of the given binary data.
     *
     * Use this factory when the source of data is an expression that can not be passed by reference.
     */
    public static function fromString(string $data): static
    {
        return new static($data);
    }

    public function readRaw(int $length): string
    {
        if ($length < 0) {
            throw new \InvalidArgumentException("Length should not be negative, {$length} given");
        }
        if ($length === 0) {
            return '';
        }
        $available = strlen($this->buffer);
        if ($available < $length) {
            throw new NetworkException(
                ['error' => "Not enough data in the buffer: {$length} bytes requested, {$available} available"]
            );
        }
        $data         = substr($this->buffer, 0, $length);
        $this->buffer = substr($this->buffer, $length);

        return $data;
    }

    public function writeRaw(string $data): void
    {
        $this->buffer .= $data;
    }

    /**
     * Returns the number of bytes that are still available for reading
     */
    public function remaining(): int
    {
        return strlen($this->buffer);
    }

    /**
     * Checks if there is nothing left to read in the buffer
     */
    public function isEmpty(): bool
    {
        return $this->buffer === '';
    }

    /**
     * Returns the content of the underlying buffer without consuming it
     */
    public function getBuffer(): string
    {
        return $this->buffer;
    }
}
