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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;

/**
 * What {@see AdminClient::deleteRecords()} should delete from one partition
 *
 * `org.apache.kafka.clients.admin.RecordsToDelete` of the Java admin client, which has exactly one way of naming
 * the records to delete - everything **before** an offset - and the same constant for "everything that is fully
 * replicated" that the wire has.
 *
 * <code>
 *   RecordsToDelete::beforeOffset(100);   // the records 0 to 99 go away, 100 becomes the low watermark
 *   RecordsToDelete::allRecords();        // everything up to the high watermark of the partition
 * </code>
 *
 * @see docs/protocol/2.8.md, section "DeleteRecords API (key 21, v0 to v2)"
 */
final class RecordsToDelete
{
    /**
     * Deletes every record up to the high watermark, {@see DeleteRecordsRequest::HIGH_WATERMARK}
     */
    public const int HIGH_WATERMARK = DeleteRecordsRequest::HIGH_WATERMARK;

    /**
     * @param int $beforeOffset Offset of the first record that survives, or {@see self::HIGH_WATERMARK}
     */
    private function __construct(public readonly int $beforeOffset) {}

    /**
     * Deletes every record whose offset is below the given one
     */
    public static function beforeOffset(int $offset): self
    {
        return new self($offset);
    }

    /**
     * Deletes every record the consumers of the partition may already have read, i.e. up to its high watermark
     */
    public static function allRecords(): self
    {
        return new self(self::HIGH_WATERMARK);
    }
}
