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

namespace Protocol\Kafka\Protocol;

/**
 * Declares a nested structure that the specification marks `"nullableVersions"`: a **single** struct field that
 * may be absent, which is one byte on the wire and then, if it is there, the structure itself.
 *
 * A nullable *array* announces its absence in its own count - the compact 0 of KIP-482, the int32 -1 below it -
 * and a nullable *string* the same way, so both are declared with {@see BinarySchema::FLAG_NULLABLE} on the field.
 * A nullable **struct** has no count to borrow: `MessageDataGenerator` @ 3.9.2 writes a leading `int8` for it,
 * `-1` for null and `1` for a structure that follows, and adds that byte to the size of the field
 * (`_size.addBytes(1)` in the `isStruct()` branch of `generateFieldSize`). Nothing else changes: a present
 * structure of a flexible version still ends in its own tagged-field section, so this marker is the plain
 * structure of a `string` scheme type with one byte in front of it.
 *
 * The first field of this protocol that needs it is the **cursor of DescribeTopicPartitions** (key 75, Kafka 3.8,
 * KIP-966), on both sides of the api - the `cursor` a request may start a page at, and the `next_cursor` an
 * answer ends a page with:
 *
 * <code>
 * return $header + [
 *     'topics'                 => ['name' => DescribeTopicPartitionsRequestTopic::class],
 *     'responsePartitionLimit' => BinarySchema::TYPE_INT32,
 *     'cursor'                 => new NullableStruct(DescribeTopicPartitionsCursor::class),
 * ];
 * </code>
 *
 * The property the marker stands on is nullable and `null` by default; a decoder writes `null` for the `-1` byte
 * and the structure for the `1`, and an encoder writes the byte back from the property.
 *
 * @see docs/protocol/4.3.md, sections "Implementation model" and "DescribeTopicPartitions API (key 75, v0)"
 */
final class NullableStruct
{
    /**
     * Byte that announces a structure follows
     */
    public const int PRESENT = 1;

    /**
     * Byte that announces the field is null
     */
    public const int ABSENT = -1;

    /**
     * @param class-string<BinarySchemaInterface> $type Class of the structure this field carries when it is there
     */
    public function __construct(public readonly string $type) {}
}
