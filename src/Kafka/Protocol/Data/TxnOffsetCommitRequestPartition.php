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

use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One partition of a TxnOffsetCommit request, i.e. one entry of the `partitions` array of a topic
 *
 * <pre>
 *   TxnOffsetCommitRequestPartition => partition offset metadata
 *     partition => INT32
 *     offset    => INT64
 *     metadata  => NULLABLE_STRING
 * </pre>
 *
 * `TXN_OFFSET_COMMIT_PARTITION_OFFSET_METADATA_REQUEST_V0` in `Protocol.java` @ 0.11.0.3. It is the entry of an
 * {@see OffsetCommitRequestPartition} **without the timestamp of the v1 request**: a transactional commit carries
 * the offset to store and the free-form metadata the consumer wants to keep with it, and nothing else. The
 * retention of the commit is the one the group coordinator applies by itself, there is no `retention_time` in this
 * api.
 *
 * @see docs/protocol/0.11.0.md, section "TxnOffsetCommit API (key 28, v0)"
 */
class TxnOffsetCommitRequestPartition implements BinarySchemaInterface
{
    /**
     * Id of the partition whose offset is committed
     */
    public int $partition;

    /**
     * Offset of the next record the group will read from that partition
     */
    public int $offset;

    /**
     * Free-form metadata the consumer keeps next to the offset, `null` for none
     */
    public ?string $metadata;

    public function __construct(int $partition, int|OffsetAndMetadata $offset, ?string $metadata = null)
    {
        $this->partition = $partition;
        $this->offset    = $offset instanceof OffsetAndMetadata ? $offset->offset : $offset;
        $this->metadata  = $offset instanceof OffsetAndMetadata ? $offset->metadata : $metadata;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'offset'    => BinarySchema::TYPE_INT64,
            'metadata'  => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
