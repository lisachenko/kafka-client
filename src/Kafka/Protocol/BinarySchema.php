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

use function count;
use function current;
use function is_array;
use function is_string;
use function key;

use Protocol\Kafka\IO\Stream;
use ReflectionClass;

use function strlen;

/**
 * BinarySchema defines the common types and the API for reading and writing the primitive types of the protocol.
 *
 * This is the 0.8 port of the engine of the `main` branch: the type constants keep their numeric values so that the
 * cascade merges upwards stay trivial, but only the types that the 0.8 protocol actually has are implemented.
 * Varints, zigzag encoding and var-arrays arrive with the 0.11 record format and are deliberately absent.
 *
 * @see docs/protocol/0.10.2.md
 */
class BinarySchema
{
    public const int TYPE_INT8      = 1;
    public const int TYPE_INT16     = 2;
    public const int TYPE_INT32     = 3;
    public const int TYPE_INT64     = 4;
    public const int TYPE_STRING    = 8;  // INT16-encoded length and then bytes of chars
    public const int TYPE_BYTEARRAY = 10; // INT32 size of data, then bytes of data, -1 as size means null

    /**
     * Use -1 as null array/string
     */
    public const int FLAG_NULLABLE = 128;

    /**
     * INT16-encoded length and then bytes of chars, -1 as size means null value
     */
    public const int TYPE_NULLABLE_STRING = self::TYPE_STRING | self::FLAG_NULLABLE;

    /**
     * Calculates the size of a single item, which can be a scalar, an array or an object
     *
     * @param mixed $schemeType BinarySchema type
     * @param mixed $value      Optional value to calculate the size of a string, array, object, etc.
     */
    public static function getSingleTypeSize(mixed $schemeType, mixed $value = null): int
    {
        // Let's check for the complex type mapping
        if (is_array($schemeType)) {
            return self::getArrayTypeSize($schemeType, $value);
        }

        // If it's a string, then we have an object with an internal scheme
        if (is_string($schemeType)) {
            return self::getObjectTypeSize($value);
        }

        switch ($schemeType) {
            case self::TYPE_INT8:
            case self::TYPE_INT16:
            case self::TYPE_INT32:
            case self::TYPE_INT64:
                return 2 ** ($schemeType - 1); // We assume the sequence 1..4 and use it as the base for 2^type

            case self::TYPE_STRING:
            case self::TYPE_NULLABLE_STRING:
                return 2 /* INT16 Size */ + ($value !== null ? strlen((string) $value) : 0);

            case self::TYPE_BYTEARRAY:
                return 4 /* INT32 Size */ + ($value !== null ? strlen((string) $value) : 0);
        }

        throw new \RuntimeException("Unknown scheme type {$schemeType}");
    }

    /**
     * Calculates the size of an array in bytes
     *
     * @param array<mixed>      $schemeType Special notation for an array
     * @param array<mixed>|null $value      Array of items, or null for nullable arrays
     */
    public static function getArrayTypeSize(array $schemeType, ?array $value = null): int
    {
        $isNullable    = !empty($schemeType[self::FLAG_NULLABLE]);
        $arrayItemType = current($schemeType);
        if ($value === null) {
            if (!$isNullable) {
                throw new \UnexpectedValueException('Received null value for not nullable array');
            }

            return self::getSingleTypeSize(self::TYPE_INT32, -1);
        }

        $size = self::getSingleTypeSize(self::TYPE_INT32, count($value));
        foreach ($value as $singleItemValue) {
            $size += self::getSingleTypeSize($arrayItemType, $singleItemValue);
        }

        return $size;
    }

    /**
     * Calculates the size of an object in bytes
     */
    public static function getObjectTypeSize(BinarySchemaInterface $object): int
    {
        $objectScheme   = $object->getScheme();
        $sizeCalculator = function (array $objectScheme) use ($object): int {
            $objectSize = 0;
            foreach ($objectScheme as $fieldKey => $schemeType) {
                $objectSize += BinarySchema::getSingleTypeSize($schemeType, $object->$fieldKey);
            }

            return $objectSize;
        };

        return $sizeCalculator->call($object, $objectScheme);
    }

    /**
     * Reads a whole object of the given class from the stream, following its scheme
     *
     * @param class-string<BinarySchemaInterface> $recordClass
     */
    public static function readObjectFromStream(string $recordClass, Stream $stream, string $path = ''): object
    {
        $scheme           = $recordClass::getScheme();
        $recordReflection = new ReflectionClass($recordClass);
        $record           = $recordReflection->newInstanceWithoutConstructor();

        $reader = function (array $scheme) use ($record, $stream, $path): void {
            foreach ($scheme as $fieldKey => $schemeType) {
                $record->$fieldKey = BinarySchema::readSingleType($schemeType, $stream, "{$path}->{$fieldKey}");
            }
        };
        $reader->call($record, $scheme);

        return $record;
    }

    /**
     * Writes a whole object to the stream, following its scheme
     */
    public static function writeObjectToStream(BinarySchemaInterface $record, Stream $stream): void
    {
        $scheme = $record->getScheme();
        $writer = function (array $scheme) use ($record, $stream): void {
            foreach ($scheme as $fieldKey => $schemeType) {
                BinarySchema::writeSingleType($schemeType, $record->$fieldKey, $stream);
            }
        };
        $writer->call($record, $scheme);
    }

    /**
     * Reads a single value of the given type from the stream
     */
    public static function readSingleType(mixed $schemeType, Stream $stream, string $path = ''): mixed
    {
        // Let's check for the complex type mapping
        if (is_array($schemeType)) {
            $arrayItemType = current($schemeType);
            $arrayKeyName  = key($schemeType);
            $isNullable    = !empty($schemeType[self::FLAG_NULLABLE]);
            $arraySize     = self::readSingleType(self::TYPE_INT32, $stream, "{$path}[size]");
            // Special handling of the null value type
            if ($arraySize === -1 && $isNullable) {
                return null;
            }

            $result = [];
            for ($index = 0; $index < $arraySize; $index++) {
                $value = self::readSingleType($arrayItemType, $stream, "{$path}[{$index}]");
                if (is_string($arrayKeyName)) {
                    $result[$value->$arrayKeyName] = $value;
                } else {
                    $result[] = $value;
                }
            }

            return $result;
        }

        // If it's a string, then we have a nested object that can be unpacked
        if (is_string($schemeType)) {
            return self::readObjectFromStream($schemeType, $stream, "{$path}:{$schemeType}");
        }

        switch ($schemeType) {
            case self::TYPE_INT8:
                // Signed, unlike the implementation on `main`, which reads 'C': the Attributes byte of a Message
                // and the error codes of the protocol are signed values
                return $stream->read('cINT8')['INT8'];

            case self::TYPE_INT16:
                $value = $stream->read('nINT16')['INT16'];
                if ($value & 0x8000) {
                    $value -= 0x10000;
                }

                return $value;

            case self::TYPE_INT32:
                $value = $stream->read('NINT32')['INT32'];
                if ($value & 0x80000000) {
                    $value -= 0x100000000;
                }

                return $value;

            case self::TYPE_INT64:
                return $stream->read('JINT64')['INT64'];

            case self::TYPE_STRING:
                return $stream->readString();

            case self::TYPE_NULLABLE_STRING:
                $stringSize = self::readSingleType(self::TYPE_INT16, $stream, "{$path}[size]");
                if ($stringSize < 0) {
                    return null;
                }

                return $stringSize === 0 ? '' : $stream->read("a{$stringSize}data")['data'];

            case self::TYPE_BYTEARRAY:
                return $stream->readByteArray();
        }

        throw new \RuntimeException("Unknown scheme type {$schemeType} received at {$path}");
    }

    /**
     * Writes a single value of the given type to the stream
     */
    public static function writeSingleType(mixed $schemeType, mixed $value, Stream $stream): void
    {
        // Let's check for the complex type mapping
        if (is_array($schemeType)) {
            $arrayItemType = current($schemeType);
            $isNullable    = !empty($schemeType[self::FLAG_NULLABLE]);
            // Special handling of null arrays
            if ($value === null) {
                if (!$isNullable) {
                    throw new \UnexpectedValueException('Received null value for not nullable array');
                }
                self::writeSingleType(self::TYPE_INT32, -1, $stream);

                return;
            }

            if (!is_array($value)) {
                $receivedType = gettype($value);
                throw new \UnexpectedValueException("Array type should receive only arrays, {$receivedType} received");
            }

            self::writeSingleType(self::TYPE_INT32, count($value), $stream);
            foreach ($value as $singleItemValue) {
                self::writeSingleType($arrayItemType, $singleItemValue, $stream);
            }

            return;
        }

        // If it's a string, then we have a nested object that can be packed into the stream
        if (is_string($schemeType)) {
            self::writeObjectToStream($value, $stream);

            return;
        }

        switch ($schemeType) {
            case self::TYPE_INT8:
                $stream->write('c', $value);

                return;
            case self::TYPE_INT16:
                $stream->write('n', $value);

                return;
            case self::TYPE_INT32:
                $stream->write('N', $value);

                return;
            case self::TYPE_INT64:
                $stream->write('J', $value);

                return;
            case self::TYPE_STRING:
                $stream->writeString((string) $value);

                return;
            case self::TYPE_NULLABLE_STRING:
                if ($value === null) {
                    self::writeSingleType(self::TYPE_INT16, -1, $stream);

                    return;
                }
                $stream->writeString((string) $value);

                return;
            case self::TYPE_BYTEARRAY:
                $stream->writeByteArray($value);

                return;
        }

        throw new \RuntimeException("Unknown scheme type {$schemeType}");
    }
}
