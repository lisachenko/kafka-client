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

namespace Protocol\Kafka\Tests\Fixture;

use Protocol\Kafka\Protocol\Request\FetchRequest;

/**
 * A Fetch request of version 2, the version that says "I understand message format v1".
 *
 * The body of version 2 is byte-identical to the one of version 1 (`FETCH_REQUEST_V2` is `FETCH_REQUEST_V1` in
 * `Protocol.java` @ 0.10.2.2) and so is the answer, so the whole api version is this one constant. What changes is
 * what the **broker** does with the log before it answers: for a request of version 0 or 1 it converts every stored
 * message of format v1 down to format v0 (`KafkaApis.handleFetchRequest`: `versionId <= 1 && getMagic(tp) > 0`
 * ⇒ `toMessageFormat(MAGIC_VALUE_V0)`), which strips the timestamps and turns the relative inner offsets of a
 * compressed set back into absolute ones.
 *
 * The Fetch versions of this client are the business of the ticket that implements Fetch v2 and v3; until then this
 * test-only subclass is what makes the message format v1 of the broker visible to the test suite, and it is the
 * reason why it lives in `tests/Fixture` and not in `src`.
 *
 * @see docs/protocol/0.10.2.md, section "MessageSet and Message"
 */
final class FetchRequestV2 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
