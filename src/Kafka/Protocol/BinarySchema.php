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

use Protocol\Kafka\Common\Utils\ByteUtils;
use Protocol\Kafka\IO\Stream;
use ReflectionClass;

use function strlen;

/**
 * BinarySchema defines the common types and the API for reading and writing the primitive types of the protocol.
 *
 * This is the engine of the `0.8.x` line carried up through the cascade: the type constants keep the names and the
 * numeric values of the pre-schema `main`, and every line implements only the types its protocol has. The boolean
 * (a single byte, `00` or `01`) arrived with Kafka 0.10 (`is_internal` of Metadata v1, `validate_only` of
 * CreateTopics v1). Kafka 0.11 adds the zigzag varints of the record batch v2 - `TYPE_VARINT_ZIGZAG`,
 * `TYPE_VARLONG_ZIGZAG` and `TYPE_VARCHAR_ZIGZAG`, encoded as `org.apache.kafka.common.utils.ByteUtils` writes them,
 * with {@see \Protocol\Kafka\Common\Utils\ByteUtils} for the zigzag step and {@see Stream::readVarint()} for the
 * bytes - and the varint-counted array (`FLAG_VARARRAY`) that the headers of a record are. No request or response of
 * 0.11 uses them, only the records inside a batch do; the *raw* varints of `main` (`TYPE_VARINT` 5, `TYPE_VARLONG` 6,
 * `TYPE_VARCHAR` 7) are not on the wire of any api of 0.11 and stay reserved numbers here.
 *
 * @see docs/protocol/2.8.md
 */
class BinarySchema
{
    public const int TYPE_INT8      = 1;
    public const int TYPE_INT16     = 2;
    public const int TYPE_INT32     = 3;
    public const int TYPE_INT64     = 4;
    // 5, 6 and 7 are TYPE_VARINT, TYPE_VARLONG and TYPE_VARCHAR of `main`: raw varints, not on the wire of Kafka 0.11
    public const int TYPE_STRING    = 8;  // INT16-encoded length and then bytes of chars
    public const int TYPE_BYTEARRAY = 10; // INT32 size of data, then bytes of data, -1 as size means null
    public const int TYPE_VARINT_ZIGZAG  = 11; // Zigzag-encoded int32 as a varint of 1 to 5 bytes (Kafka 0.11)
    public const int TYPE_VARLONG_ZIGZAG = 12; // Zigzag-encoded int64 as a varint of 1 to 10 bytes (Kafka 0.11)
    public const int TYPE_VARCHAR_ZIGZAG = 13; // Zigzag varint size of data, then bytes of data, -1 as size means null
    public const int TYPE_BOOLEAN   = 20; // A single byte: 0 is false, anything else is true (Kafka 0.10)

    /**
     * Array notation key: the element count is a zigzag varint instead of an int32 (the headers of a record, Kafka 0.11)
     */
    public const int FLAG_VARARRAY = 14;

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

            case self::TYPE_BOOLEAN:
                return 1;

            case self::TYPE_VARINT_ZIGZAG:
                return ByteUtils::sizeOfVarint((int) $value);

            case self::TYPE_VARLONG_ZIGZAG:
                return ByteUtils::sizeOfVarlong((int) $value);

            case self::TYPE_STRING:
            case self::TYPE_NULLABLE_STRING:
                return 2 /* INT16 Size */ + ($value !== null ? strlen((string) $value) : 0);

            case self::TYPE_BYTEARRAY:
                return 4 /* INT32 Size */ + ($value !== null ? strlen((string) $value) : 0);

            case self::TYPE_VARCHAR_ZIGZAG:
                $length = $value !== null ? strlen((string) $value) : -1;

                return ByteUtils::sizeOfVarint($length) + max($length, 0);
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
        $countType     = self::arrayCountType($schemeType);
        $arrayItemType = current($schemeType);
        if ($value === null) {
            if (!$isNullable) {
                throw new \UnexpectedValueException('Received null value for not nullable array');
            }

            return self::getSingleTypeSize($countType, -1);
        }

        $size = self::getSingleTypeSize($countType, count($value));
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
            $arraySize     = self::readSingleType(self::arrayCountType($schemeType), $stream, "{$path}[size]");
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

            case self::TYPE_BOOLEAN:
                // Types.BOOLEAN of the Java client reads any non-zero byte as true and always writes 0 or 1
                return $stream->read('CBOOLEAN')['BOOLEAN'] !== 0;

            case self::TYPE_VARINT_ZIGZAG:
                return ByteUtils::decodeZigZag($stream->readVarint());

            case self::TYPE_VARLONG_ZIGZAG:
                return ByteUtils::decodeZigZag($stream->readVarlong());

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

            case self::TYPE_VARCHAR_ZIGZAG:
                $length = self::readSingleType(self::TYPE_VARINT_ZIGZAG, $stream, "{$path}[size]");
                if ($length < 0) {
                    return null;
                }

                return $length === 0 ? '' : $stream->read("a{$length}data")['data'];
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
            $countType     = self::arrayCountType($schemeType);
            // Special handling of null arrays
            if ($value === null) {
                if (!$isNullable) {
                    throw new \UnexpectedValueException('Received null value for not nullable array');
                }
                self::writeSingleType($countType, -1, $stream);

                return;
            }

            if (!is_array($value)) {
                $receivedType = gettype($value);
                throw new \UnexpectedValueException("Array type should receive only arrays, {$receivedType} received");
            }

            self::writeSingleType($countType, count($value), $stream);
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
            case self::TYPE_BOOLEAN:
                $stream->write('C', $value ? 1 : 0);

                return;
            case self::TYPE_VARINT_ZIGZAG:
                $stream->writeVarint(ByteUtils::encodeZigZag((int) $value, 32));

                return;
            case self::TYPE_VARLONG_ZIGZAG:
                $stream->writeVarlong(ByteUtils::encodeZigZag((int) $value, 64));

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
            case self::TYPE_VARCHAR_ZIGZAG:
                if ($value === null) {
                    self::writeSingleType(self::TYPE_VARINT_ZIGZAG, -1, $stream);

                    return;
                }
                $value = (string) $value;
                self::writeSingleType(self::TYPE_VARINT_ZIGZAG, strlen($value), $stream);
                $stream->writeBuffer($value);

                return;
        }

        throw new \RuntimeException("Unknown scheme type {$schemeType}");
    }

    /**
     * Returns the type of the element count of an array notation: an int32 unless FLAG_VARARRAY is set
     *
     * A var-array counts its elements with a *zigzag* varint (`DefaultRecord.writeTo` @ 0.11.0.3 writes the number
     * of headers with `ByteUtils.writeVarint`), so three headers are `06`, not `03`.
     *
     * @param array<mixed> $schemeType
     */
    private static function arrayCountType(array $schemeType): int
    {
        return empty($schemeType[self::FLAG_VARARRAY]) ? self::TYPE_INT32 : self::TYPE_VARINT_ZIGZAG;
    }
}
