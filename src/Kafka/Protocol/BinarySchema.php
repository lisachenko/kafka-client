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
use Protocol\Kafka\IO\StringStream;
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
 * **Kafka 2.4 (KIP-482) added a second encoding of the same types**, and it is not a set of new types but a mode:
 * from the `flexibleVersions` of an api on, *every* string, byte array and array of that version announces its
 * length as an **unsigned varint of `length + 1`** (`0` meaning `null`) instead of an int16/int32, and *every*
 * structure of it ends in a **tagged-field section** ({@see TaggedField}). There is no version of Kafka 2.8.2 that
 * mixes the two, so the engine asks the message once - {@see FlexibleSchemaInterface::isFlexible()} - and hands the
 * answer down to every nested structure through the `$flexible` parameter of its methods. A DTO therefore never
 * needs a version of its own: the same `FetchRequestTopic` scheme is written plainly in a Fetch v11 and compactly
 * in a Fetch v12, and unpacks to the same object either way.
 *
 * Two types stay out of that mode, and both of them are on the wire of a flexible frame: the `client_id` of the
 * request header v2 ({@see self::TYPE_STRING_NEVER_COMPACT}, `"flexibleVersions": "none"` in
 * `RequestHeaderData.json` @ 2.8.2) and the tagged-field section of a header, which is declared explicitly with
 * {@see self::TYPE_TAG_BUFFER} because it sits in the *middle* of a frame rather than at the end of a structure.
 *
 * @see docs/protocol/2.8.md, section "Implementation model"
 */
class BinarySchema
{
    public const int TYPE_INT8      = 1;
    public const int TYPE_INT16     = 2;
    public const int TYPE_INT32     = 3;
    public const int TYPE_INT64     = 4;
    // 5, 6 and 7 are TYPE_VARINT, TYPE_VARLONG and TYPE_VARCHAR of `main`: raw varints, not on the wire of any api
    public const int TYPE_STRING    = 8;  // INT16-encoded length and then bytes of chars, compact when flexible
    /**
     * INT16-encoded length and then bytes of chars, **even inside a flexible structure**
     *
     * The one field of Kafka 2.8.2 that is declared `"flexibleVersions": "none"`: the `client_id` of the request
     * header v2. It keeps the old prefix so that a broker can read the header of an ApiVersions request whose
     * version it does not know yet - which is the request a client sends before it knows anything about the peer.
     */
    public const int TYPE_STRING_NEVER_COMPACT = 9;
    public const int TYPE_BYTEARRAY = 10; // INT32 size of data, then bytes of data, -1 as size means null
    public const int TYPE_VARINT_ZIGZAG  = 11; // Zigzag-encoded int32 as a varint of 1 to 5 bytes (Kafka 0.11)
    public const int TYPE_VARLONG_ZIGZAG = 12; // Zigzag-encoded int64 as a varint of 1 to 10 bytes (Kafka 0.11)
    public const int TYPE_VARCHAR_ZIGZAG = 13; // Zigzag varint size of data, then bytes of data, -1 as size means null
    public const int TYPE_BOOLEAN   = 20; // A single byte: 0 is false, anything else is true (Kafka 0.10)
    /**
     * 16 raw bytes, the `uuid` of the JSON message specifications (Kafka 2.8, KIP-516)
     *
     * A fixed-width type that no length prefix precedes, so the compact encoding does not touch it. The value is
     * the raw 16 bytes; `00000000-0000-0000-0000-000000000000` is the "no topic id" of the specification.
     */
    public const int TYPE_UUID      = 21;
    /**
     * A tagged-field section in the *middle* of a frame: the tag buffer of a request header v2 or a response
     * header v1 (KIP-482)
     *
     * The section at the *end* of a structure is appended by the engine itself and is declared with
     * {@see TaggedField} entries (or with nothing at all, for the empty one every flexible structure carries);
     * this type exists for the one place where a tag buffer is followed by more fields of the same object. The
     * value is the map `tag => raw bytes` of the fields that were read, empty for the headers this client writes.
     */
    public const int TYPE_TAG_BUFFER = 22;

    /**
     * An IEEE 754 double in **big-endian** byte order, the `float64` of the JSON message specifications (Kafka 2.6)
     *
     * `Type.FLOAT64` of the Java client writes it with `ByteBuffer.putDouble()`, i.e. network order, and the only
     * fields of Kafka 2.8.2 that use it are the quota values of DescribeClientQuotas (48) and AlterClientQuotas
     * (49). A fixed-width type, so the compact encoding does not touch it.
     */
    public const int TYPE_FLOAT64 = 23;

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
     * Name of the optional property that keeps the tagged fields a structure did not know
     *
     * A class that declares it - {@see \Protocol\Kafka\Protocol\AbstractProtocolMessage} does, and any DTO can with
     * {@see PreservesUnknownTaggedFields} - gets the `tag => raw bytes` of every tag the scheme does not declare and
     * writes them back untouched, so that a frame from a newer broker survives a decode and encode round trip. A
     * class that does not declare it drops those fields, which is what the Java client does with a message it
     * re-serializes without `unknownTaggedFields`.
     */
    private const string UNKNOWN_TAGGED_FIELDS = 'unknownTaggedFields';

    /**
     * Calculates the size of a single item, which can be a scalar, an array or an object
     *
     * @param mixed $schemeType BinarySchema type
     * @param mixed $value      Optional value to calculate the size of a string, array, object, etc.
     * @param bool  $flexible   Whether the structure this value belongs to uses the compact types of KIP-482
     */
    public static function getSingleTypeSize(mixed $schemeType, mixed $value = null, bool $flexible = false): int
    {
        // Let's check for the complex type mapping
        if (is_array($schemeType)) {
            return self::getArrayTypeSize($schemeType, $value, $flexible);
        }

        // A nested object that the specification does not have: its fields belong to the structure around it, so
        // it carries no tagged-field section of its own even in a flexible version
        if ($schemeType instanceof InlineStruct) {
            return self::objectSize($value, $flexible, false);
        }

        // If it's a string, then we have an object with an internal scheme
        if (is_string($schemeType)) {
            return self::getObjectTypeSize($value, $flexible);
        }

        switch ($schemeType) {
            case self::TYPE_INT8:
            case self::TYPE_INT16:
            case self::TYPE_INT32:
            case self::TYPE_INT64:
                return 2 ** ($schemeType - 1); // We assume the sequence 1..4 and use it as the base for 2^type

            case self::TYPE_BOOLEAN:
                return 1;

            case self::TYPE_FLOAT64:
                return 8;

            case self::TYPE_UUID:
                return 16;

            case self::TYPE_VARINT_ZIGZAG:
                return ByteUtils::sizeOfVarint((int) $value);

            case self::TYPE_VARLONG_ZIGZAG:
                return ByteUtils::sizeOfVarlong((int) $value);

            case self::TYPE_STRING:
            case self::TYPE_NULLABLE_STRING:
            case self::TYPE_BYTEARRAY:
                $length = $value !== null ? strlen((string) $value) : -1;
                if ($flexible) {
                    return self::compactLengthSize($length) + max($length, 0);
                }
                $prefix = $schemeType === self::TYPE_BYTEARRAY ? 4 /* INT32 Size */ : 2 /* INT16 Size */;

                return $prefix + max($length, 0);

            case self::TYPE_STRING_NEVER_COMPACT:
                return 2 /* INT16 Size */ + ($value !== null ? strlen((string) $value) : 0);

            case self::TYPE_TAG_BUFFER:
                return self::taggedSectionSize(is_array($value) ? $value : []);

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
     * @param bool              $flexible   Whether the element count is a compact one
     */
    public static function getArrayTypeSize(array $schemeType, ?array $value = null, bool $flexible = false): int
    {
        $isNullable    = !empty($schemeType[self::FLAG_NULLABLE]);
        $isCompact     = $flexible && empty($schemeType[self::FLAG_VARARRAY]);
        $countType     = self::arrayCountType($schemeType);
        $arrayItemType = current($schemeType);
        if ($value === null) {
            if (!$isNullable) {
                throw new \UnexpectedValueException('Received null value for not nullable array');
            }

            return $isCompact ? 1 /* the compact count 0 */ : self::getSingleTypeSize($countType, -1);
        }

        $size = $isCompact
            ? ByteUtils::sizeOfUnsignedVarint(count($value) + 1)
            : self::getSingleTypeSize($countType, count($value));
        foreach ($value as $singleItemValue) {
            $size += self::getSingleTypeSize($arrayItemType, $singleItemValue, $flexible);
        }

        return $size;
    }

    /**
     * Calculates the size of an object in bytes
     *
     * @param bool|null $flexible Encoding of the object; `null` asks the object itself, which is what a top-level
     *                            message answers with its {@see FlexibleSchemaInterface::isFlexible()}
     */
    public static function getObjectTypeSize(BinarySchemaInterface $object, ?bool $flexible = null): int
    {
        return self::objectSize($object, $flexible ?? self::isFlexibleClass($object::class), true);
    }

    /**
     * Calculates the size of an object, with or without the tagged-field section of a flexible structure
     */
    private static function objectSize(BinarySchemaInterface $object, bool $flexible, bool $withTaggedSection): int
    {
        [$plainScheme, $taggedScheme] = self::splitScheme($object::getScheme());

        $sizeCalculator = function (array $objectScheme) use ($object, $flexible): int {
            $objectSize = 0;
            foreach ($objectScheme as $fieldKey => $schemeType) {
                $objectSize += BinarySchema::getSingleTypeSize($schemeType, $object->$fieldKey, $flexible);
            }

            return $objectSize;
        };
        $objectSize = $sizeCalculator->call($object, $plainScheme);

        if ($flexible && $withTaggedSection) {
            $objectSize += self::taggedSectionSize(self::taggedFieldsOf($object, $taggedScheme));
        }

        return $objectSize;
    }

    /**
     * Reads a whole object of the given class from the stream, following its scheme
     *
     * @param class-string<BinarySchemaInterface> $recordClass
     * @param bool|null                           $flexible Encoding of the object; `null` asks the class itself
     */
    public static function readObjectFromStream(
        string $recordClass,
        Stream $stream,
        string $path = '',
        ?bool $flexible = null
    ): object {
        return self::readObject(
            $recordClass,
            $stream,
            $path,
            $flexible ?? self::isFlexibleClass($recordClass),
            true
        );
    }

    /**
     * Reads an object, with or without the tagged-field section of a flexible structure
     *
     * @param class-string<BinarySchemaInterface> $recordClass
     */
    private static function readObject(
        string $recordClass,
        Stream $stream,
        string $path,
        bool $flexible,
        bool $withTaggedSection
    ): object {
        [$plainScheme, $taggedScheme] = self::splitScheme($recordClass::getScheme());
        $recordReflection             = new ReflectionClass($recordClass);
        $record                       = $recordReflection->newInstanceWithoutConstructor();

        $reader = function (array $scheme) use ($record, $stream, $path, $flexible): void {
            foreach ($scheme as $fieldKey => $schemeType) {
                $record->$fieldKey = BinarySchema::readSingleType(
                    $schemeType,
                    $stream,
                    "{$path}->{$fieldKey}",
                    $flexible
                );
            }
        };
        $reader->call($record, $plainScheme);

        if ($flexible && $withTaggedSection) {
            self::readTaggedSection($record, $taggedScheme, $stream, $path);
        }

        return $record;
    }

    /**
     * Writes a whole object to the stream, following its scheme
     *
     * @param bool|null $flexible Encoding of the object; `null` asks the object itself
     */
    public static function writeObjectToStream(
        BinarySchemaInterface $record,
        Stream $stream,
        ?bool $flexible = null
    ): void {
        self::writeObject($record, $stream, $flexible ?? self::isFlexibleClass($record::class), true);
    }

    /**
     * Writes an object, with or without the tagged-field section of a flexible structure
     */
    private static function writeObject(
        BinarySchemaInterface $record,
        Stream $stream,
        bool $flexible,
        bool $withTaggedSection
    ): void {
        [$plainScheme, $taggedScheme] = self::splitScheme($record::getScheme());

        $writer = function (array $scheme) use ($record, $stream, $flexible): void {
            foreach ($scheme as $fieldKey => $schemeType) {
                BinarySchema::writeSingleType($schemeType, $record->$fieldKey, $stream, $flexible);
            }
        };
        $writer->call($record, $plainScheme);

        if ($flexible && $withTaggedSection) {
            self::writeTaggedSection(self::taggedFieldsOf($record, $taggedScheme), $stream);
        }
    }

    /**
     * Reads a single value of the given type from the stream
     *
     * @param bool $flexible Whether the structure this value belongs to uses the compact types of KIP-482
     */
    public static function readSingleType(
        mixed $schemeType,
        Stream $stream,
        string $path = '',
        bool $flexible = false
    ): mixed {
        // Let's check for the complex type mapping
        if (is_array($schemeType)) {
            $arrayItemType = current($schemeType);
            $arrayKeyName  = key($schemeType);
            $isNullable    = !empty($schemeType[self::FLAG_NULLABLE]);
            $isCompact     = $flexible && empty($schemeType[self::FLAG_VARARRAY]);
            if ($isCompact) {
                $announced = $stream->readUnsignedVarint();
                if ($announced === 0) {
                    if (!$isNullable) {
                        throw new \UnexpectedValueException("Received null value for not nullable array at {$path}");
                    }

                    return null;
                }
                $arraySize = $announced - 1;
            } else {
                $arraySize = self::readSingleType(self::arrayCountType($schemeType), $stream, "{$path}[size]");
                // Special handling of the null value type
                if ($arraySize === -1 && $isNullable) {
                    return null;
                }
            }

            $result = [];
            for ($index = 0; $index < $arraySize; $index++) {
                $value = self::readSingleType($arrayItemType, $stream, "{$path}[{$index}]", $flexible);
                if (is_string($arrayKeyName)) {
                    $result[$value->$arrayKeyName] = $value;
                } else {
                    $result[] = $value;
                }
            }

            return $result;
        }

        // A nested object the specification does not have, i.e. a group of fields of this structure
        if ($schemeType instanceof InlineStruct) {
            return self::readObject($schemeType->type, $stream, "{$path}:{$schemeType->type}", $flexible, false);
        }

        // If it's a string, then we have a nested object that can be unpacked
        if (is_string($schemeType)) {
            return self::readObjectFromStream($schemeType, $stream, "{$path}:{$schemeType}", $flexible);
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

            case self::TYPE_FLOAT64:
                return $stream->read('EFLOAT64')['FLOAT64'];

            case self::TYPE_UUID:
                return (string) $stream->read('a16data')['data'];

            case self::TYPE_VARINT_ZIGZAG:
                return ByteUtils::decodeZigZag($stream->readVarint());

            case self::TYPE_VARLONG_ZIGZAG:
                return ByteUtils::decodeZigZag($stream->readVarlong());

            case self::TYPE_STRING:
                if ($flexible) {
                    $value = self::readCompactBytes($stream);
                    if ($value === null) {
                        throw new \UnexpectedValueException("Received null value for not nullable string at {$path}");
                    }

                    return $value;
                }

                return $stream->readString();

            case self::TYPE_NULLABLE_STRING:
                if ($flexible) {
                    return self::readCompactBytes($stream);
                }
                $stringSize = self::readSingleType(self::TYPE_INT16, $stream, "{$path}[size]");
                if ($stringSize < 0) {
                    return null;
                }

                return $stringSize === 0 ? '' : $stream->read("a{$stringSize}data")['data'];

            case self::TYPE_STRING_NEVER_COMPACT:
                return $stream->readString();

            case self::TYPE_BYTEARRAY:
                if ($flexible) {
                    return self::readCompactBytes($stream);
                }

                return $stream->readByteArray();

            case self::TYPE_TAG_BUFFER:
                return self::readTagBuffer($stream, $path);

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
     *
     * @param bool $flexible Whether the structure this value belongs to uses the compact types of KIP-482
     */
    public static function writeSingleType(
        mixed $schemeType,
        mixed $value,
        Stream $stream,
        bool $flexible = false
    ): void {
        // Let's check for the complex type mapping
        if (is_array($schemeType)) {
            $arrayItemType = current($schemeType);
            $isNullable    = !empty($schemeType[self::FLAG_NULLABLE]);
            $isCompact     = $flexible && empty($schemeType[self::FLAG_VARARRAY]);
            $countType     = self::arrayCountType($schemeType);
            // Special handling of null arrays
            if ($value === null) {
                if (!$isNullable) {
                    throw new \UnexpectedValueException('Received null value for not nullable array');
                }
                if ($isCompact) {
                    $stream->writeUnsignedVarint(0);
                } else {
                    self::writeSingleType($countType, -1, $stream);
                }

                return;
            }

            if (!is_array($value)) {
                $receivedType = gettype($value);
                throw new \UnexpectedValueException("Array type should receive only arrays, {$receivedType} received");
            }

            if ($isCompact) {
                $stream->writeUnsignedVarint(count($value) + 1);
            } else {
                self::writeSingleType($countType, count($value), $stream);
            }
            foreach ($value as $singleItemValue) {
                self::writeSingleType($arrayItemType, $singleItemValue, $stream, $flexible);
            }

            return;
        }

        // A nested object the specification does not have, i.e. a group of fields of this structure
        if ($schemeType instanceof InlineStruct) {
            self::writeObject($value, $stream, $flexible, false);

            return;
        }

        // If it's a string, then we have a nested object that can be packed into the stream
        if (is_string($schemeType)) {
            self::writeObjectToStream($value, $stream, $flexible);

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
            case self::TYPE_FLOAT64:
                $stream->write('E', (float) $value);

                return;
            case self::TYPE_UUID:
                $uuid = (string) $value;
                if (strlen($uuid) !== 16) {
                    throw new \UnexpectedValueException('A uuid is 16 raw bytes, ' . strlen($uuid) . ' given');
                }
                $stream->writeBuffer($uuid);

                return;
            case self::TYPE_VARINT_ZIGZAG:
                $stream->writeVarint(ByteUtils::encodeZigZag((int) $value, 32));

                return;
            case self::TYPE_VARLONG_ZIGZAG:
                $stream->writeVarlong(ByteUtils::encodeZigZag((int) $value, 64));

                return;
            case self::TYPE_STRING:
                if ($flexible) {
                    self::writeCompactBytes((string) $value, $stream);

                    return;
                }
                $stream->writeString((string) $value);

                return;
            case self::TYPE_NULLABLE_STRING:
                if ($flexible) {
                    self::writeCompactBytes($value === null ? null : (string) $value, $stream);

                    return;
                }
                if ($value === null) {
                    self::writeSingleType(self::TYPE_INT16, -1, $stream);

                    return;
                }
                $stream->writeString((string) $value);

                return;
            case self::TYPE_STRING_NEVER_COMPACT:
                $stream->writeString((string) $value);

                return;
            case self::TYPE_BYTEARRAY:
                if ($flexible) {
                    self::writeCompactBytes($value === null ? null : (string) $value, $stream);

                    return;
                }
                $stream->writeByteArray($value);

                return;
            case self::TYPE_TAG_BUFFER:
                self::writeTaggedSection(is_array($value) ? $value : [], $stream);

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
     * Splits a scheme into the fields that sit at a fixed offset and the tagged fields that travel at the end
     *
     * A tagged field is declared like any other field, with a {@see TaggedField} descriptor as its type, and the
     * engine takes it out of the sequence here: on the wire it belongs to the tagged-field section of its
     * structure, in ascending order of its tag, whatever order the scheme declares it in. A structure that is
     * **not** flexible ignores the declaration altogether - `taggedVersions` is always a subset of
     * `flexibleVersions` in the specifications, so a tagged field simply does not exist in an older version, and
     * its property keeps the default of the class.
     *
     * @param array<string, mixed> $scheme
     *
     * @return array{array<string, mixed>, array<int, array{string, TaggedField}>}
     */
    private static function splitScheme(array $scheme): array
    {
        $plain  = [];
        $tagged = [];
        foreach ($scheme as $fieldKey => $schemeType) {
            if ($schemeType instanceof TaggedField) {
                $tagged[$schemeType->tag] = [$fieldKey, $schemeType];
            } else {
                $plain[$fieldKey] = $schemeType;
            }
        }
        ksort($tagged);

        return [$plain, $tagged];
    }

    /**
     * Returns the encoded tagged fields of an object, as `tag => bytes`, ascending
     *
     * A field whose value is the default of its declaration is **left out**, which is what `Message.write()` of the
     * Java client does and what makes the byte-exact round trip of a captured frame possible. The tags that the
     * scheme does not know are kept as they arrived, if the class has somewhere to keep them.
     *
     * @param array<int, array{string, TaggedField}> $taggedScheme
     *
     * @return array<int, string>
     */
    private static function taggedFieldsOf(BinarySchemaInterface $object, array $taggedScheme): array
    {
        $encoder = function (array $taggedScheme) use ($object): array {
            $fields = [];
            foreach ($taggedScheme as $tag => [$fieldKey, $descriptor]) {
                $value = $object->$fieldKey;
                if ($value === $descriptor->default) {
                    continue;
                }
                $buffer = new StringStream();
                BinarySchema::writeSingleType($descriptor->type, $value, $buffer, true);
                $fields[$tag] = $buffer->getBuffer();
            }
            if (property_exists($object, BinarySchema::UNKNOWN_TAGGED_FIELDS)) {
                $fields += $object->{BinarySchema::UNKNOWN_TAGGED_FIELDS};
            }

            return $fields;
        };

        $fields = $encoder->call($object, $taggedScheme);
        ksort($fields);

        return $fields;
    }

    /**
     * Reads the tagged-field section at the end of a flexible structure into the object
     *
     * A tag the scheme does not declare is **skipped by its announced size** - that is the whole point of the
     * mechanism, and it is what lets a client of this release read a frame of a later one - and kept as raw bytes
     * when the class has a place for them.
     *
     * @param array<int, array{string, TaggedField}> $taggedScheme
     */
    private static function readTaggedSection(
        object $record,
        array $taggedScheme,
        Stream $stream,
        string $path
    ): void {
        $unknown = self::readTagBuffer($stream, $path, $taggedScheme, $record);
        if (property_exists($record, self::UNKNOWN_TAGGED_FIELDS)) {
            $setter = function (object $record, array $unknown): void {
                $record->{BinarySchema::UNKNOWN_TAGGED_FIELDS} = $unknown;
            };
            $setter->call($record, $record, $unknown);
        }
    }

    /**
     * Reads a tagged-field section and returns the fields no scheme entry claimed
     *
     * @param array<int, array{string, TaggedField}> $taggedScheme Fields to decode into $record, by tag
     *
     * @return array<int, string> The raw value of every other tag, by tag
     */
    private static function readTagBuffer(
        Stream $stream,
        string $path,
        array $taggedScheme = [],
        ?object $record = null
    ): array {
        $numberOfFields = $stream->readUnsignedVarint();
        $unknown        = [];
        for ($index = 0; $index < $numberOfFields; $index++) {
            $tag  = $stream->readUnsignedVarint();
            $size = $stream->readUnsignedVarint();
            $raw  = $size === 0 ? '' : (string) $stream->read("a{$size}data")['data'];
            if ($record !== null && isset($taggedScheme[$tag])) {
                [$fieldKey, $descriptor] = $taggedScheme[$tag];
                $value                   = self::readSingleType(
                    $descriptor->type,
                    new StringStream($raw),
                    "{$path}->{$fieldKey}",
                    true
                );
                $reader = function (object $record, string $fieldKey, mixed $value): void {
                    $record->$fieldKey = $value;
                };
                $reader->call($record, $record, $fieldKey, $value);

                continue;
            }
            $unknown[$tag] = $raw;
        }

        return $unknown;
    }

    /**
     * Writes a tagged-field section: the number of fields, then `tag`, `size` and value of each, tags ascending
     *
     * @param array<int, string> $fields
     */
    private static function writeTaggedSection(array $fields, Stream $stream): void
    {
        $stream->writeUnsignedVarint(count($fields));
        foreach ($fields as $tag => $bytes) {
            $stream->writeUnsignedVarint($tag);
            $stream->writeUnsignedVarint(strlen($bytes));
            $stream->writeBuffer($bytes);
        }
    }

    /**
     * Returns the size of a tagged-field section in bytes
     *
     * @param array<int, string> $fields
     */
    private static function taggedSectionSize(array $fields): int
    {
        $size = ByteUtils::sizeOfUnsignedVarint(count($fields));
        foreach ($fields as $tag => $bytes) {
            $length = strlen($bytes);
            $size += ByteUtils::sizeOfUnsignedVarint($tag) + ByteUtils::sizeOfUnsignedVarint($length) + $length;
        }

        return $size;
    }

    /**
     * Reads a compact string or byte array: the unsigned varint `length + 1`, `0` meaning `null`
     */
    private static function readCompactBytes(Stream $stream): ?string
    {
        $announced = $stream->readUnsignedVarint();
        if ($announced === 0) {
            return null;
        }
        $length = $announced - 1;

        return $length === 0 ? '' : (string) $stream->read("a{$length}data")['data'];
    }

    /**
     * Writes a compact string or byte array: the unsigned varint `length + 1`, `0` for `null`
     */
    private static function writeCompactBytes(?string $value, Stream $stream): void
    {
        if ($value === null) {
            $stream->writeUnsignedVarint(0);

            return;
        }
        $stream->writeUnsignedVarint(strlen($value) + 1);
        $stream->writeBuffer($value);
    }

    /**
     * Returns the number of bytes the compact length prefix of a string or byte array occupies
     */
    private static function compactLengthSize(int $length): int
    {
        return ByteUtils::sizeOfUnsignedVarint(max($length, -1) + 1);
    }

    /**
     * Whether the given class stands for a flexible version of its api
     *
     * @param class-string $class
     */
    private static function isFlexibleClass(string $class): bool
    {
        return is_a($class, FlexibleSchemaInterface::class, true) && $class::isFlexible();
    }

    /**
     * Returns the type of the element count of an array notation: an int32 unless FLAG_VARARRAY is set
     *
     * A var-array counts its elements with a *zigzag* varint (`DefaultRecord.writeTo` @ 0.11.0.3 writes the number
     * of headers with `ByteUtils.writeVarint`), so three headers are `06`, not `03`. A **compact** array of a
     * flexible version is neither of the two - its count is an unsigned varint of `count + 1` - and is handled
     * where it is read and written.
     *
     * @param array<mixed> $schemeType
     */
    private static function arrayCountType(array $schemeType): int
    {
        return empty($schemeType[self::FLAG_VARARRAY]) ? self::TYPE_INT32 : self::TYPE_VARINT_ZIGZAG;
    }
}
