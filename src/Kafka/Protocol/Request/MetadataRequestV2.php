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

namespace Protocol\Kafka\Protocol\Request;

/**
 * Metadata request of version 2 (Kafka 0.10.1), the nullable topic array without the auto-creation flag
 *
 * <pre>
 *   Metadata Request (Version: 2) => [topics]
 *     topics => NULLABLE_ARRAY of STRING
 * </pre>
 *
 * `METADATA_REQUEST_V2 = METADATA_REQUEST_V1` in `Protocol.java` @ 0.11.0.3 - the frame is the one of
 * {@see MetadataRequestV1} - and the version exists because its ANSWER carries the `ClusterId` on top
 * ({@see MetadataResponseV2}).
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
final class MetadataRequestV2 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * The `allow_auto_topic_creation` of version 4 is not on the wire in this version, and a broker behaves as if
     * it were `true`; the constructor therefore does not ask for it.
     *
     * @param list<string>|null $topics        Topics to fetch the metadata for, null asks for every topic
     * @param string            $clientId      A user specified identifier for the client making the request
     * @param int               $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(?array $topics = null, string $clientId = '', int $correlationId = 0)
    {
        parent::__construct($topics, true, $clientId, $correlationId);
    }
}
