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

namespace Protocol\Kafka\Tests\Compliance;

use function is_array;
use function is_string;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\InlineStruct;
use Protocol\Kafka\Protocol\TaggedField;
use ReflectionProperty;

/**
 * Flattens a decoded protocol message into the plain structure that a vector file stores.
 *
 * The walk follows the scheme of the message, so the result contains exactly the fields the wire format has, in the
 * order the wire format has them, with nested objects as nested maps and arrays as arrays. Raw byte fields - the
 * message set of the Produce and Fetch apis, the varint-prefixed key, value and header of a record of the
 * message format v2, and the 16 bytes of a `uuid` - become `{"$bytes": "<hex>"}`, because JSON cannot carry
 * binary.
 *
 * The two descriptors of the flexible encoding are unwrapped on the way: a {@see TaggedField} is documented as the
 * value of its own type - a vector shows what the tagged field carries, not that it is tagged - and an
 * {@see InlineStruct} as the nested map its object is. The tag buffer of a header is a field of the scheme like any
 * other and shows up as the (usually empty) map of the tags that were read.
 */
final class MessageFields
{
    /**
     * Marker key that a vector file uses for a raw byte field
     */
    public const string BYTES_KEY = '$bytes';

    /**
     * Returns the fields of a decoded message, following its scheme
     *
     * @return array<string, mixed>
     */
    public static function of(BinarySchemaInterface $message): array
    {
        $fields = [];
        foreach ($message::getScheme() as $field => $schemeType) {
            $fields[$field] = self::valueOf($schemeType, self::propertyValue($message, $field));
        }

        return $fields;
    }

    /**
     * Converts a single value of the given scheme type
     */
    private static function valueOf(mixed $schemeType, mixed $value): mixed
    {
        // A tagged field of a flexible version is an ordinary value behind its descriptor, and a nested object that
        // the specification does not have is an ordinary nested object
        if ($schemeType instanceof TaggedField) {
            return self::valueOf($schemeType->type, $value);
        }
        if ($schemeType instanceof InlineStruct) {
            return self::of($value);
        }

        if (is_array($schemeType)) {
            if ($value === null) {
                return null;
            }
            $itemType = current($schemeType);
            $result   = [];
            foreach ($value as $key => $item) {
                $result[$key] = self::valueOf($itemType, $item);
            }

            return $result;
        }

        // A nested structure that is not there at all - a tagged field of a flexible version that the writer left
        // out, which is what its default "nothing to report" looks like after decoding - stays null
        if (is_string($schemeType)) {
            return $value === null ? null : self::of($value);
        }

        // A uuid is 16 raw bytes as well - the topic ids of KIP-516 - and JSON cannot carry them either
        $isBytes = $schemeType === BinarySchema::TYPE_BYTEARRAY
            || $schemeType === BinarySchema::TYPE_VARCHAR_ZIGZAG
            || $schemeType === BinarySchema::TYPE_UUID;
        if ($isBytes && $value !== null) {
            return [self::BYTES_KEY => bin2hex((string) $value)];
        }

        return $value;
    }

    /**
     * Reads a property of a message, whatever its visibility is
     */
    private static function propertyValue(object $message, string $field): mixed
    {
        $property = new ReflectionProperty($message, $field);

        return $property->isInitialized($message) ? $property->getValue($message) : null;
    }
}
