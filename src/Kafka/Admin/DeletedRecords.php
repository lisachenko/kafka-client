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

use Protocol\Kafka\Protocol\Data\DeleteRecordsResponsePartition;

/**
 * What {@see AdminClient::deleteRecords()} deleted from one partition
 *
 * `org.apache.kafka.clients.admin.DeletedRecords` of the Java admin client: the new **low watermark** of the
 * partition, i.e. the offset of the first record that is still readable. It is the offset the request asked for
 * unless the broker had to cap it at the high watermark, and it is the same number that an Offsets request with
 * `EARLIEST` and the `log_start_offset` of a Fetch v5 answer report for that partition.
 *
 * @see docs/protocol/0.11.0.md, section "DeleteRecords API (key 21, v0)"
 */
final class DeletedRecords
{
    /**
     * @param int $lowWatermark Offset of the first record that is still readable
     */
    public function __construct(public readonly int $lowWatermark) {}

    /**
     * Builds the result from the partition entry of a DeleteRecords answer
     */
    public static function fromResponsePartition(DeleteRecordsResponsePartition $partition): self
    {
        return new self($partition->lowWatermark);
    }
}
