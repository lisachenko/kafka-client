<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Fetch request topic DTO
 *
 * FetchResponseAbortedTransaction => producer_id first_offset
 *   producer_id => INT64
 *   first_offset => INT64
 *
 * @since Version 4
 */
class FetchResponseAbortedTransaction implements BinarySchemaInterface
{
    /**
     * The producer id associated with the aborted transactions
     *
     * @var integer
     */
    public $producerId;

    /**
     * The first offset in the aborted transaction
     *
     * @var integer
     */
    public $firstOffset;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'producerId'  => BinarySchema::TYPE_INT64,
            'firstOffset' => BinarySchema::TYPE_INT64,
        ];
    }
}
