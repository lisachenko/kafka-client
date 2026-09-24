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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One batch of acknowledgements of a ShareFetch or ShareAcknowledge request (keys 78 and 79, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   AcknowledgementBatch => first_offset last_offset [acknowledge_types]
 *     first_offset      => INT64
 *     last_offset       => INT64    -- inclusive
 *     acknowledge_types => INT8     -- one for the whole batch, or one per offset
 * </pre>
 *
 * The `AcknowledgementBatch` of `ShareFetchRequest.json` and `ShareAcknowledgeRequest.json` @ 4.1.0, *"Array of
 * acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject"*. The array holds **one** type that stands for every offset
 * of the batch, or **one type per offset** - `KafkaApis.validateAcknowledgementBatches` @ 4.3.1 refuses anything else
 * with the 42, and so a batch whose first offset lies behind the last one, a batch that starts before the previous
 * batch of the same partition ended, an empty array and a type outside 0 to 3 at version 1 and 0 to 4 at version 2.
 *
 * * {@see self::ACCEPT} - the record was processed: it is done for the group;
 * * {@see self::RELEASE} - give the record back: it becomes available again, and its delivery count stays;
 * * {@see self::REJECT} - the record cannot be processed: it is archived and never delivered again;
 * * {@see self::GAP} - an offset of the acquired range that holds no record (a compacted or transactional log);
 * * {@see self::RENEW} - keep the record acquired and start its acquisition lock over (version 2, Kafka 4.2,
 *   KIP-1222): only in a request that says `is_renew_ack`, which is the 42 of the partition otherwise.
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1 and v2)"
 */
final class ShareAcknowledgementBatch implements BinarySchemaInterface
{
    /**
     * An offset that holds no record
     */
    public const int GAP = 0;

    /**
     * The record was processed (`AcknowledgeType.ACCEPT`)
     */
    public const int ACCEPT = 1;

    /**
     * The record is given back to be delivered again (`AcknowledgeType.RELEASE`)
     */
    public const int RELEASE = 2;

    /**
     * The record is archived without being delivered again (`AcknowledgeType.REJECT`)
     */
    public const int REJECT = 3;

    /**
     * The record stays acquired and its acquisition lock starts over (`AcknowledgeType.RENEW`)
     *
     * @since Version 2 of ShareFetch and ShareAcknowledge (Kafka 4.2, KIP-1222)
     */
    public const int RENEW = 4;

    /**
     * @param int       $firstOffset      First offset of the batch
     * @param int       $lastOffset       Last offset of the batch, inclusive
     * @param list<int> $acknowledgeTypes One type for the whole batch, or one per offset
     */
    public function __construct(
        public int $firstOffset = 0,
        public int $lastOffset = 0,
        public array $acknowledgeTypes = []
    ) {}

    /**
     * Acknowledges every offset from `$firstOffset` to `$lastOffset` with the same type
     */
    public static function of(int $firstOffset, int $lastOffset, int $acknowledgeType): self
    {
        return new self($firstOffset, $lastOffset, [$acknowledgeType]);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'firstOffset'      => BinarySchema::TYPE_INT64,
            'lastOffset'       => BinarySchema::TYPE_INT64,
            'acknowledgeTypes' => [BinarySchema::TYPE_INT8],
        ];
    }
}
