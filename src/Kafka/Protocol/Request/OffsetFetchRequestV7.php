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
 * OffsetFetch request of version 7 (Kafka 2.5, KIP-447): one group per request
 *
 * Version 8 (Kafka 3.0) moved the group id and the topic array into a `groups` array, so that one request can ask
 * for the committed offsets of several groups at once; this version names its single group at the top level and is
 * the highest one a broker below Kafka 3.0 serves. {@see OffsetFetchRequestV8} is the batch without the
 * member of KIP-848, and {@see OffsetFetchRequest} the version 9 this client sends.
 *
 * @see docs/protocol/4.3.md, section "Stable offsets and the 88 of KIP-447 (Kafka 2.5)"
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 */
final class OffsetFetchRequestV7 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
