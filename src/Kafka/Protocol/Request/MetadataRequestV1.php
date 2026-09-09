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
 * Metadata request of version 1 (Kafka 0.10.0), the first one with a nullable topic array
 *
 * <pre>
 *   Metadata Request (Version: 1) => [topics]
 *     topics => NULLABLE_ARRAY of STRING
 * </pre>
 *
 * `METADATA_REQUEST_V2 = METADATA_REQUEST_V1` in `Protocol.java` @ 0.10.2.2: the frame of this class differs from
 * the one of {@see MetadataRequest} in the version field of the header alone. It exists because the ANSWER differs -
 * a version 1 answer has no `ClusterId` ({@see MetadataResponseV1}) - so a client that asks with this class has to
 * read the answer with the matching response class.
 *
 * @see docs/protocol/0.11.0.md, section "Metadata API (key 3, v0, v1 and v2)"
 */
final class MetadataRequestV1 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
