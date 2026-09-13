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
 * Basic class for all responses
 *
 * Response Header (Version: 0) => correlation_id
 *   correlation_id => INT32
 *
 * Response Header (Version: 1) => correlation_id TAG_BUFFER
 *
 * The answer of a **flexible** version (KIP-482, Kafka 2.4) carries a tagged-field section behind the correlation
 * id, and {@see self::getHeaderVersion()} is the `ApiKeys.responseHeaderVersion()` of the Java client. It has
 * exactly one exception, written out by hand in the generator: the **ApiVersions** response always uses the header
 * v0, flexible body or not, so that a client which asked for a version the broker does not serve can read the
 * error code 35 out of a frame whose header it can always parse.
 *
 * @see docs/protocol/2.8.md, section "Responses"
 */
abstract class AbstractResponse extends AbstractProtocolMessage
{
    /**
     * The common response header, a correlation id and nothing else
     */
    public const int HEADER_V0 = 0;

    /**
     * The response header of a flexible version: the correlation id plus a tagged-field section (KIP-482)
     */
    public const int HEADER_V1 = 1;

    /**
     * Version of the response header this api version is answered with (`ApiKeys.responseHeaderVersion()` @ 2.8.2)
     */
    public static function getHeaderVersion(): int
    {
        return static::isFlexible() ? self::HEADER_V1 : self::HEADER_V0;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = [
            'messageSize'   => BinarySchema::TYPE_INT32,
            'correlationId' => BinarySchema::TYPE_INT32,
        ];
        if (static::getHeaderVersion() >= self::HEADER_V1) {
            $header['headerTaggedFields'] = BinarySchema::TYPE_TAG_BUFFER;
        }

        return $header;
    }

    /**
     * Returns the correlation id that the broker echoed back from the matching request
     */
    public function getCorrelationId(): int
    {
        return $this->correlationId;
    }
}
