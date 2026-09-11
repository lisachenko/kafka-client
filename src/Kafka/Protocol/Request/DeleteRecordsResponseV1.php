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
 * DeleteRecords response of version 1 (key 21)
 *
 * The **plain** encoding of the very frame version 2 speaks: `int16`-prefixed strings, `int32`-prefixed arrays
 * and no tagged-field section anywhere. Version 1 (Kafka 2.0, KIP-219) is byte-identical to version 0 and only
 * promises that the client waits out `throttle_time_ms` itself; version 2 (Kafka 2.6) is the first **flexible**
 * version of this api - `DeleteRecordsResponse.json` @ 2.8.2 declares `"flexibleVersions": "2+"` - and adds no
 * field either, see {@see DeleteRecordsResponse}.
 *
 * The class inherits {@see DeleteRecordsResponse::FLEXIBLE_VERSION} (2) and is therefore **not** flexible: the
 * engine asks `VERSION >= FLEXIBLE_VERSION`, and 1 is not.
 *
 * @see docs/protocol/2.8.md, sections "DeleteRecords API (key 21, v0 to v2)" and
 *      "Flexible versions in the engine (KIP-482)"
 */
final class DeleteRecordsResponseV1 extends DeleteRecordsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
