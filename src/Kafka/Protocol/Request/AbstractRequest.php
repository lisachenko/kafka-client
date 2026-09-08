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
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Basic class for all requests
 *
 * Request Header => api_key api_version correlation_id client_id
 *   api_key        => INT16
 *   api_version    => INT16
 *   correlation_id => INT32
 *   client_id      => STRING
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
     * The version of the API. (INT16)
     */
    protected int $apiVersion;

    /**
     * Payload of a request class that still packs itself by hand instead of declaring a scheme
     */
    private ?string $legacyPayload = null;

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

        if (static::hasLegacyPayloadWriter()) {
            $this->legacyPayload = $this->packPayload();
            $this->messageSize   = strlen($this->legacyPayload);
        } else {
            $this->messageSize = BinarySchema::getObjectTypeSize($this) - 4 /* INT32 MessageSize */;
        }
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'messageSize'   => BinarySchema::TYPE_INT32,
            'apiKey'        => BinarySchema::TYPE_INT16,
            'apiVersion'    => BinarySchema::TYPE_INT16,
            'correlationId' => BinarySchema::TYPE_INT32,
            'clientId'      => BinarySchema::TYPE_STRING,
        ];
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

    /**
     * @inheritdoc
     */
    protected function packInto(Stream $stream): void
    {
        if ($this->legacyPayload !== null) {
            $stream->write('N', $this->messageSize);
            $stream->writeBuffer($this->legacyPayload);

            return;
        }

        parent::packInto($stream);
    }

    /**
     * Packs the request header, byte for byte as {@see AbstractRequest::getScheme()} describes it.
     *
     * The request classes that have not been migrated to a scheme yet override this method and concatenate their own
     * pack()-ed body to the result of `parent::packPayload()`.
     *
     * @deprecated Declare a {@see BinarySchema} scheme instead, this hook disappears once every request class is
     *             described by a scheme.
     */
    protected function packPayload(): string
    {
        // self:: on purpose: this must stay the header of the base class even for a subclass with its own scheme
        $headerScheme = self::getScheme();
        unset($headerScheme['messageSize']);

        $stream = new StringStream();
        foreach ($headerScheme as $fieldName => $schemeType) {
            BinarySchema::writeSingleType($schemeType, $this->$fieldName, $stream);
        }

        return $stream->getBuffer();
    }

    /**
     * Checks whether the concrete class still packs its payload by hand instead of declaring a scheme
     */
    private static function hasLegacyPayloadWriter(): bool
    {
        return new \ReflectionMethod(static::class, 'packPayload')->getDeclaringClass()->getName() !== self::class;
    }
}
