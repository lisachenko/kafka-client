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

namespace Protocol\Kafka\Common\Record;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The Key of a control record: the version of the key schema and the {@see ControlRecordType} it announces.
 *
 * <pre>
 *   Key => Version Type
 *     Version => int16 (0)
 *     Type    => int16
 * </pre>
 *
 * The version exists so that a later broker can add fields to the key without breaking a client that reads it: a
 * client parses the two fields it knows and ignores whatever follows them, and a type it does not know is
 * {@see ControlRecordType::UNKNOWN}, which it silently drops.
 *
 * @see docs/protocol/2.8.md, section "RecordBatch (message format v2)"
 * @see org/apache/kafka/common/record/ControlRecordType.java @ 0.11.0.3
 */
class ControlRecordKey implements BinarySchemaInterface
{
    /**
     * @param int $version Version of the key schema, 0 in Kafka 0.11
     * @param int $type    One of the {@see ControlRecordType} constants
     */
    public function __construct(
        public int $version = ControlRecordType::CURRENT_KEY_VERSION,
        public int $type = ControlRecordType::UNKNOWN,
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'version' => BinarySchema::TYPE_INT16,
            'type'    => BinarySchema::TYPE_INT16,
        ];
    }
}
