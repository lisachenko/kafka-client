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
 * Metadata request of version 8 (key 3)
 *
 * The **plain** encoding of the same question version 9 asks: the nullable topic array, the
 * `allow_auto_topic_creation` of version 4 and the two booleans of KIP-430, with `int16`-prefixed strings, an
 * `int32`-prefixed array and the request header v1. Version 9 (Kafka 2.4) is the first flexible version of this
 * api - `MetadataRequest.json` @ 2.8.2 declares `"flexibleVersions": "9+"` - and its frame carries the very same
 * values as compact types, see {@see MetadataRequest}.
 *
 * The class inherits {@see MetadataRequest::FLEXIBLE_VERSION} (9) and is therefore **not** flexible: the engine
 * asks `VERSION >= FLEXIBLE_VERSION`, and 8 is not.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v9)" and
 *      "Flexible versions in the engine (KIP-482)"
 */
final class MetadataRequestV8 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
