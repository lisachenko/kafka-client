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
 * Metadata request, version 5 (key 3)
 *
 * <pre>
 *   Metadata Request (Version: 5) => [topics] allow_auto_topic_creation
 * </pre>
 *
 * The frame of version 5 (Kafka 1.0, KIP-112/113) is the frame of version 6, byte for byte: `MetadataRequest.json`
 * @ 2.8.2 has no field between version 4 and version 8. This is the highest version a **Kafka 1.1.1** broker
 * serves and the version the 1.x line of this client sent; a 2.8.2 broker still serves it, and throttles it
 * exactly as it throttles version 6, see {@see MetadataRequest}.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v6)"
 */
final class MetadataRequestV5 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
