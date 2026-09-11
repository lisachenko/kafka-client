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

use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Basic class for all requests
 *
 * Request Header (Version: 1) => api_key api_version correlation_id client_id
 *   api_key        => INT16
 *   api_version    => INT16
 *   correlation_id => INT32
 *   client_id      => STRING
 *
 * Request Header (Version: 2) => api_key api_version correlation_id client_id TAG_BUFFER
 *
 * The header of a **flexible** version (KIP-482, Kafka 2.4) is the same four fields plus a tagged-field section,
 * and the `client_id` keeps its int16 length prefix even there - `RequestHeader.json` @ 2.8.2 marks the field
 * `"flexibleVersions": "none"` so that a broker can read the header of an ApiVersions request whose version it does
 * not know. {@see self::getHeaderVersion()} is the `ApiKeys.requestHeaderVersion()` of the Java client, including
 * its one exception: version 0 of ControlledShutdown carries no client id at all.
 *
 * @see docs/protocol/2.8.md, section "Requests"
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
     * Request header without a client id, used by version 0 of ControlledShutdown and by nothing else
     */
    public const int HEADER_V0 = 0;

    /**
     * The common request header of Kafka 0.8 to 2.3, and of every non-flexible version afterwards
     */
    public const int HEADER_V1 = 1;

    /**
     * The request header of a flexible version: the header v1 plus a tagged-field section (KIP-482)
     */
    public const int HEADER_V2 = 2;

    /**
     * The version of the API. (INT16)
     */
    protected int $apiVersion;

    /**
     * Global request counter, only used by {@see AbstractRequest::nextCorrelationId()}
     */
    private static int $counter = 0;

    /**
     * @param int    $apiKey        The id of the request type (INT16)
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(protected int $apiKey, protected string $clientId = '', int $correlationId = 0)
    {
        $this->apiVersion    = static::VERSION;
        $this->correlationId = $correlationId;
        $this->messageSize   = BinarySchema::getObjectTypeSize($this) - 4 /* INT32 MessageSize */;
    }

    /**
     * Version of the request header this api version is sent with (`ApiKeys.requestHeaderVersion()` @ 2.8.2)
     *
     * A flexible version uses the header v2, every other one the header v1; the single frame of the protocol that
     * uses the header v0 - ControlledShutdown v0, which has no client id - says so by overriding this method.
     */
    public static function getHeaderVersion(): int
    {
        return static::isFlexible() ? self::HEADER_V2 : self::HEADER_V1;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = [
            'messageSize'   => BinarySchema::TYPE_INT32,
            'apiKey'        => BinarySchema::TYPE_INT16,
            'apiVersion'    => BinarySchema::TYPE_INT16,
            'correlationId' => BinarySchema::TYPE_INT32,
        ];
        if (static::getHeaderVersion() >= self::HEADER_V1) {
            // The client id is never compact, whatever the version of the frame is: "flexibleVersions": "none"
            $header['clientId'] = BinarySchema::TYPE_STRING_NEVER_COMPACT;
        }
        if (static::getHeaderVersion() >= self::HEADER_V2) {
            $header['headerTaggedFields'] = BinarySchema::TYPE_TAG_BUFFER;
        }

        return $header;
    }

    /**
     * Returns the next value of the process-wide correlation id sequence.
     *
     * The correlation id of a request is always the one the caller supplied; this helper only exists for the callers
     * that do not maintain their own sequence yet.
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

}
