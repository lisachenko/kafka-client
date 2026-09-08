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
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;

/**
 * Basic class for all responses.
 *
 * Every response is framed as follows:
 *
 * <pre>
 *   Response => Size CorrelationId ResponseBody
 *     Size          => int32
 *     CorrelationId => int32
 * </pre>
 *
 * @see docs/protocol/0.8.2.md, section "Responses"
 */
abstract class AbstractResponse extends AbstractProtocolMessage
{
    /**
     * Size of the response header that follows the Size field, i.e. the correlation id (INT32)
     */
    private const int HEADER_SIZE = 4;

    /**
     * A user-supplied integer value that will be passed back with the response (INT32)
     *
     * @var integer
     */
    public $correlationId;

    /**
     * Reads one complete response frame from the stream and unpacks it.
     *
     * The whole frame is buffered first, so the body parser can never read past the response boundary and a
     * malformed body does not desynchronize the connection.
     */
    final public static function unpackFrom(Stream $stream): static
    {
        $messageSize = $stream->readInt32();
        if ($messageSize < self::HEADER_SIZE) {
            throw new NetworkException(['error' => "Invalid response size: {$messageSize}"]);
        }

        $frame = StringStream::fromString($stream->readRaw($messageSize));

        $self                = new static();
        $self->correlationId = $frame->readInt32();
        $self->unpackBody($frame);

        return $self;
    }

    /**
     * Returns the correlation id that the broker echoed back from the matching request
     */
    public function getCorrelationId(): int
    {
        return (int) $this->correlationId;
    }

    /**
     * Reads the response body (everything after the correlation id) from the given stream.
     *
     * This is the contract for the typed protocol classes, the default implementation reads nothing.
     */
    protected function unpackBody(Stream $stream): void
    {
        // nothing here
    }
}
