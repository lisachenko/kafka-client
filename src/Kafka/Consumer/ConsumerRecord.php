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

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\Common\Record\Record;

/**
 * A record that {@see KafkaConsumer::poll()} received, together with the values a deserializer made out of it.
 *
 * {@see Record} carries the key and the value as the opaque byte strings that travel over the wire, which is what
 * `key.deserializer` and `value.deserializer` turn into application-level values - and those are not necessarily
 * strings. This subclass keeps the raw bytes in place and adds the deserialized values next to them, so the result
 * of a poll() stays a list of records whatever the deserializers return.
 *
 * Records of a poll() are only wrapped into this class when at least one deserializer is configured.
 */
class ConsumerRecord extends Record
{
    /**
     * @param string      $topic             Topic this record was read from
     * @param int         $partition         Partition this record was read from
     * @param string|null $value             Raw bytes of the value, as they lie in the log
     * @param string|null $key               Raw bytes of the key, as they lie in the log
     * @param int         $attributes        Attributes byte of the message that carried this record
     * @param int|null    $offset            Offset of this record in the log
     * @param mixed       $deserializedKey   Key as `key.deserializer` returned it, the raw key without one
     * @param mixed       $deserializedValue Value as `value.deserializer` returned it, the raw value without one
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        ?string $value = null,
        ?string $key = null,
        int $attributes = 0,
        ?int $offset = null,
        public readonly mixed $deserializedKey = null,
        public readonly mixed $deserializedValue = null,
    ) {
        parent::__construct($value, $key, $attributes, $offset);
    }

    /**
     * Builds a consumer record from the record that the fetch returned and the deserialized key and value
     */
    public static function fromRecord(
        Record $record,
        string $topic,
        int $partition,
        mixed $deserializedKey,
        mixed $deserializedValue
    ): self {
        return new self(
            $topic,
            $partition,
            $record->value,
            $record->key,
            $record->attributes,
            $record->offset,
            $deserializedKey,
            $deserializedValue
        );
    }
}
