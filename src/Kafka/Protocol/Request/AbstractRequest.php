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

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;

/**
 * Basic class for all requests.
 *
 * Every request is framed as follows:
 *
 * <pre>
 *   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestBody
 *     ApiKey        => int16
 *     ApiVersion    => int16
 *     CorrelationId => int32
 *     ClientId      => string
 * </pre>
 *
 * @see docs/protocol/0.8.2.md, section "Requests"
 */
abstract class AbstractRequest extends AbstractProtocolMessage
{
    /**
     * Numeric id of the API being invoked, overridden in children classes (INT16)
     */
    public const int API_KEY = -1;

    /**
     * Version of API request, could be overridden in children classes (INT16)
     */
    public const int VERSION = 0;

    /**
     * The id of the request type. (INT16)
     */
    protected int $apiKey;

    /**
     * The version of the API. (INT16)
     */
    protected int $apiVersion;

    /**
     * A user-supplied integer value that will be passed back with the response (INT32)
     */
    protected int $correlationId;

    /**
     * Global request counter, used only by {@see AbstractRequest::nextCorrelationId()}
     */
    private static int $counter = 0;

    /**
     * @param int|null $apiKey        Api key of the request, defaults to the API_KEY constant of the class
     * @param string   $clientId      A user specified identifier for the client making the request
     * @param int      $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(?int $apiKey = null, protected string $clientId = '', int $correlationId = 0)
    {
        $this->apiKey        = $apiKey ?? static::API_KEY;
        $this->apiVersion    = static::VERSION;
        $this->correlationId = $correlationId;

        $this->setMessageData($this->packPayload());
    }

    /**
     * Returns the next value of the process-wide correlation id sequence.
     *
     * The correlation id is always supplied by the caller, this helper only exists for the callers that do not
     * maintain their own sequence yet.
     */
    public static function nextCorrelationId(): int
    {
        return self::$counter++;
    }

    public function getApiKey(): int
    {
        return $this->apiKey;
    }

    public function getApiVersion(): int
    {
        return $this->apiVersion;
    }

    public function getCorrelationId(): int
    {
        return $this->correlationId;
    }

    public function getClientId(): string
    {
        return $this->clientId;
    }

    /**
     * Writes the request header to the given stream.
     */
    final protected function packHeader(Stream $stream): void
    {
        $stream->writeInt16($this->apiKey);
        $stream->writeInt16($this->apiVersion);
        $stream->writeInt32($this->correlationId);
        $stream->writeString($this->clientId);
    }

    /**
     * Writes the request body (everything after the header) to the given stream.
     *
     * This is the contract for the typed protocol classes, the default implementation writes an empty body.
     */
    protected function packBody(Stream $stream): void
    {
        // nothing here
    }

    /**
     * Implementation of packing the payload: the request header followed by the request body.
     *
     * Legacy protocol classes override this method and concatenate their own `pack()`-ed body to the result of
     * `parent::packPayload()`, which stays byte-identical to the header written by {@see self::packHeader()}.
     */
    protected function packPayload(): string
    {
        $buffer = '';
        $stream = new StringStream($buffer);

        $this->packHeader($stream);
        $this->packBody($stream);

        return $buffer;
    }
}
