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
 * A header of a record: a key-value pair of metadata that travels next to the key and the value (KIP-82).
 *
 * <pre>
 *   Header => HeaderKey HeaderValue
 *     HeaderKeyLen   => varint
 *     HeaderKey      => string
 *     HeaderValueLen => varint
 *     HeaderValue    => data
 * </pre>
 *
 * Both fields are length-prefixed by a zigzag varint, i.e. {@see BinarySchema::TYPE_VARCHAR_ZIGZAG}. The **key** is
 * a UTF-8 string that is never null on the wire - `DefaultRecord.writeTo()` refuses a null key and
 * `DefaultRecord.readHeaders()` refuses a negative key length - while the **value** is a nullable byte array, so a
 * header may carry a key alone. Headers only exist in the message format v2 of Kafka 0.11: the formats v0 and v1
 * have no place to put them, which is why {@see MessageSet::fromRecords()} ignores them.
 *
 * @see docs/protocol/1.1.md, section "RecordBatch (message format v2)"
 * @see org/apache/kafka/common/header/internals/RecordHeader.java @ 0.11.0.3
 */
class Header implements BinarySchemaInterface
{
    /**
     * @param string      $key   Name of the header, a UTF-8 string; never null on the wire
     * @param string|null $value Contents of the header as an opaque byte array; null is a valid value
     */
    public function __construct(
        public string $key = '',
        public ?string $value = null,
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'key'   => BinarySchema::TYPE_VARCHAR_ZIGZAG,
            'value' => BinarySchema::TYPE_VARCHAR_ZIGZAG,
        ];
    }
}
