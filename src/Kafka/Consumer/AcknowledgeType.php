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

namespace Protocol\Kafka\Consumer;

use InvalidArgumentException;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;

/**
 * How a share consumer acknowledges a record it was delivered (KIP-932, KIP-1222)
 *
 * `AcknowledgeType` of the Java client @ 4.3.1, the argument of {@see KafkaShareConsumer::acknowledge()}. The value
 * of a case is the id that travels in the `acknowledge_types` of an acknowledgement batch of ShareFetch and
 * ShareAcknowledge ({@see ShareAcknowledgementBatch}): the type 0 `Gap` of the wire is not an acknowledgement an
 * application gives - the share consumer sends it by itself for an acquired offset that holds no record, a control
 * record of a transaction or an offset compaction removed.
 *
 * - {@see self::ACCEPT}: the record was processed; it is never delivered again.
 * - {@see self::RELEASE}: the record was not processed; it is delivered again, to this member or another one, with a
 *   delivery count one higher, until the group config `share.delivery.count.limit` archives it.
 * - {@see self::REJECT}: the record cannot be processed; it is archived at once and never delivered again.
 * - {@see self::RENEW} (ShareFetch and ShareAcknowledge **v2**, Kafka 4.2, KIP-1222): the record is still being
 *   processed; its acquisition lock starts over, and the next {@see KafkaShareConsumer::poll()} returns it again to be
 *   acknowledged once more.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
enum AcknowledgeType: int
{
    /**
     * The record was consumed successfully
     */
    case ACCEPT = ShareAcknowledgementBatch::ACCEPT;

    /**
     * The record was not consumed successfully: release it for another delivery attempt
     */
    case RELEASE = ShareAcknowledgementBatch::RELEASE;

    /**
     * The record was not consumed successfully: reject it and do not release it for another delivery attempt
     */
    case REJECT = ShareAcknowledgementBatch::REJECT;

    /**
     * The record is still being processed: renew the acquisition lock so processing can continue (KIP-1222)
     */
    case RENEW = ShareAcknowledgementBatch::RENEW;

    /**
     * Returns the type of a wire id, the `AcknowledgeType.forId()` of the Java client
     *
     * @throws InvalidArgumentException For the type 0 `Gap` and every id above 4
     */
    public static function forId(int $id): self
    {
        return self::tryFrom($id) ?? throw new InvalidArgumentException("Unknown acknowledge type id: {$id}");
    }

    /**
     * Returns the lowercase name the Java client prints for the type, e.g. `accept`
     */
    public function typeName(): string
    {
        return strtolower($this->name);
    }
}
