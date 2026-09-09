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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce request Topic-Partition DTO
 *
 * <pre>
 *   Partition MessageSetSize MessageSet
 *     Partition      => int32
 *     MessageSetSize => int32
 * </pre>
 *
 * A message set is a raw byte region, not a length-prefixed array of structures, therefore it travels as a BYTEARRAY
 * whose int32 length prefix is exactly the `MessageSetSize` field of the spec. This is the 0.8 counterpart of the
 * `recordBatch` field of the `main` branch.
 *
 * @see docs/protocol/0.10.2.md, sections "Produce API (key 0, v0)" and "MessageSet and Message"
 */
class ProduceRequestPartition implements BinarySchemaInterface
{
    /**
     * The partition this request entry corresponds to.
     */
    public int $partition = 0;

    /**
     * Encoded message set for this topic-partition.
     */
    public string $messageSet = '';

    /**
     * @param int                $partition  Number of the partition to produce to
     * @param string|\Stringable $messageSet Encoded message set, typically a Common\Record\MessageSet instance
     */
    public function __construct(int $partition = 0, string|\Stringable $messageSet = '')
    {
        $this->partition  = $partition;
        $this->messageSet = (string) $messageSet;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'  => BinarySchema::TYPE_INT32,
            'messageSet' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
