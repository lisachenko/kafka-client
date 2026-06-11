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

namespace Protocol\Kafka\Common\Record;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * A record in kafka is a key-value pair with a small amount of associated metadata.
 *
 * Record =>
 *   Length => Varint
 *   Attributes => Int8
 *   TimestampDelta => Varlong
 *   OffsetDelta => Varint
 *   Key => Bytes
 *   Value => Bytes
 *   Headers => [HeaderKey HeaderValue]
 *     HeaderKey => String
 *     HeaderValue => Bytes
 *
 * Note that in this schema, the Bytes and String types use a variable length integer to represent
 * the length of the field. The array type used for the headers also uses a Varint for the number of
 * headers.
 *
 * The current record attributes are depicted below:
 *
 *  ----------------
 *  | Unused (0-7) |
 *  ----------------
 *
 * The offset and timestamp deltas compute the difference relative to the base offset and
 * base timestamp of the batch that this record is contained in.
 *
 * @since 0.11.0
 */
class Record implements BinarySchemaInterface
{
    /**
     * Length of this message
     *
     * @var integer
     */
    public $length = 0;

    /**
     * Record level attributes are presently unused.
     *
     * @var integer
     */
    public $attributes = 0;

    /**
     * The timestamp delta of the record in the batch.
     *
     * The timestamp of each Record in the RecordBatch is its 'TimestampDelta' + 'FirstTimestamp'.
     *
     * @var integer
     * @since Version 2 of Record structure
     */
    public $timestampDelta = 0;

    /**
     * The offset delta of the record in the batch.
     *
     * The offset of each Record in the Batch is its 'OffsetDelta' + 'FirstOffset'.
     *
     * @var integer
     *
     * @since Version 2 of Record (Record) structure
     */
    public $offsetDelta = 0;

    /**
     * The key is an optional message key that was used for partition assignment. The key can be null.
     *
     * @var string
     */
    public $key;

    /**
     * The value is the actual message contents as an opaque byte array.
     *
     * Kafka supports recursive messages in which case this may itself contain a message set. The message can be null.
     *
     * @var string
     */
    public $value;

    /**
     * Application level record level headers.
     *
     * @since Version 2 of Record (Record) structure
     * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers
     *
     * @var Header[]
     */
    public $headers = [];

    public static function fromValue($value, $attributes = 0): static
    {
        $message = new static();

        $message->value          = $value;
        $message->timestampDelta = (int) (microtime(true) * 1000 - $_SERVER['REQUEST_TIME_FLOAT'] * 1000);
        $message->attributes     = $attributes;
        $message->length         = BinarySchema::getObjectTypeSize($message) - 1;
        /* Varint 0 length always equal to 1 */;

        return $message;
    }

    public static function getScheme(): array
    {
        return [
            'length'         => BinarySchema::TYPE_VARINT_ZIGZAG,
            'attributes'     => BinarySchema::TYPE_INT8,
            'timestampDelta' => BinarySchema::TYPE_VARLONG_ZIGZAG,
            'offsetDelta'    => BinarySchema::TYPE_VARINT_ZIGZAG,
            'key'            => BinarySchema::TYPE_VARCHAR_ZIGZAG,
            'value'          => BinarySchema::TYPE_VARCHAR_ZIGZAG,
            'headers'        => ['key' => Header::class, BinarySchema::FLAG_VARARRAY => true],
        ];
    }
}
