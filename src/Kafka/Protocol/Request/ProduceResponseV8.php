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
 * Produce response of version 8 (key 0)
 *
 * The last **plain** version of the api: the same fields as version 9 - the `record_errors` and the
 * `error_message` of KIP-467 included - written with `INT16`-prefixed strings, `INT32`-counted arrays, an
 * `INT32`-prefixed record set and no tagged-field section anywhere. Version 9 (Kafka 2.8) is the flexible
 * version of KIP-482, see {@see ProduceResponse}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v9)"
 */
final class ProduceResponseV8 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
