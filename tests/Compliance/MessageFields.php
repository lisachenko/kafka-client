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
use ReflectionProperty;

/**
 * Flattens a decoded protocol message into the plain structure that a vector file stores.
 *
 * The walk follows the scheme of the message, so the result contains exactly the fields the wire format has, in the
 * order the wire format has them, with nested objects as nested maps and arrays as arrays. Raw byte fields - the
 * message set of the Produce and Fetch apis, and the varint-prefixed key, value and header of a record of the
 * message format v2 - become `{"$bytes": "<hex>"}`, because JSON cannot carry binary.
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

        if (is_string($schemeType)) {
            return self::of($value);
        }

        $isBytes = $schemeType === BinarySchema::TYPE_BYTEARRAY || $schemeType === BinarySchema::TYPE_VARCHAR_ZIGZAG;
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
