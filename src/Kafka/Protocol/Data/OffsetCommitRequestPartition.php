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
 * OffsetCommitRequestPartition DTO
 *
 * OffsetCommitRequestPartition => partition offset metadata
 *   partition => INT32
 *   offset => INT64
 *   metadata => NULLABLE_STRING
 */
class OffsetCommitRequestPartition implements BinarySchemaInterface
{
    /**
     * The partition this request entry corresponds to.
     * @var int
     */
    public $partition;

    /**
     * The offset assigned to the first message in the message set appended to this partition.
     * @var int
     */
    public $offset;

    /**
     * Any associated metadata the client wants to keep.
     * @var null|string
     */
    public $metadata;

    public function __construct(int $partition, int $offset, ?string $metadata = null)
    {
        $this->partition = $partition;
        $this->offset    = $offset;
        $this->metadata  = $metadata;
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
