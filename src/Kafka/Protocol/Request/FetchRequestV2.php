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
 * Fetch API (key 1), version 2
 *
 * <pre>
 *   FetchRequest (Version: 2) => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
 * </pre>
 *
 * Version 2 (Kafka 0.10.0) is byte-identical to version 1 in both directions - `FETCH_REQUEST_V2` is
 * `FETCH_REQUEST_V1` and `FETCH_RESPONSE_V2` is `FETCH_RESPONSE_V1` in `Protocol.java` @ 0.10.2.2 - so this class
 * only lowers the version constant, which drops the request-level `MaxBytes` of version 3.
 *
 * The whole meaning of the version is what the **broker** does with the log before it answers: from version 2 on
 * the client states that it understands message format v1, and the broker stops converting the stored messages
 * down to format v0 (`KafkaApis.handleFetchRequest`: `versionId <= 1 && getMagic(tp) > 0` ⇒
 * `toMessageFormat(MAGIC_VALUE_V0)`), i.e. the answer keeps its timestamps and the relative inner offsets of a
 * compressed set.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v12)" and "MessageSet and Message"
 */
final class FetchRequestV2 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
