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

use Protocol\Kafka\Protocol\Request\StreamGroupRequest;

class StringStream extends AbstractStream
{
    /**
     * Internal binary buffer
     */
    private ?string $buffer = null;

    /**
     * String stream constructor.
     *
     * @param string $stringBuffer Buffer to write to or read from
     */
    public function __construct(&$stringBuffer)
    {
        $this->buffer = &$stringBuffer;
    }

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
     * Joins current stream to the group (for stream_select)
     *
     * @param StreamGroupRequest $group Instance of group
     *
     * @return void
     */
    public function joinGroup(StreamGroupRequest $group): void
    {
        $group->registerHandle($this, $this->buffer);
    }
}
