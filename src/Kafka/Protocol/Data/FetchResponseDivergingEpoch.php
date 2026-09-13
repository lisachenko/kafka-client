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
 * Where the log of the fetcher and the log of the leader are known to diverge, the `EpochEndOffset` of KIP-595
 *
 * The **tag 0** of a Fetch v12 partition entry (Kafka 2.7). A fetcher states the epoch of the last record it
 * really read in {@see FetchRequestTopicPartition::$lastFetchedEpoch}; when that epoch and the fetch offset do
 * not match the leader's own log, the leader answers this structure instead of records: the largest epoch and
 * its end offset **from which the two logs are known to differ**. The fetcher truncates its log to that offset
 * and continues from there - the truncation detection of KIP-320 without a second request.
 *
 * Both fields default to `-1`, and the whole structure is left out of an answer that has nothing to report,
 * which is what a tagged field is for.
 *
 * @see docs/protocol/2.8.md, section "Epoch validation in the fetch itself (v12, KIP-595)"
 */
class FetchResponseDivergingEpoch implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO belongs to
     */
    public const int VERSION = 12;

    /**
     * Value of both fields when the leader reports no divergence at all
     */
    public const int UNDEFINED = -1;

    /**
     * Largest epoch from which the two logs are known to differ
     */
    public int $epoch = self::UNDEFINED;

    /**
     * Offset at which that epoch ends, i.e. the offset the fetcher has to truncate to
     */
    public int $endOffset = self::UNDEFINED;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'epoch'     => BinarySchema::TYPE_INT32,
            'endOffset' => BinarySchema::TYPE_INT64,
        ];
    }
}
