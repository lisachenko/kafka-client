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
 * Declares one **tagged field** of a flexible structure (KIP-482, Kafka 2.4).
 *
 * A tagged field is optional data that travels at the end of a structure instead of at a fixed offset, so that a
 * later release can add it to a version that already exists: the section is an unsigned varint count followed by a
 * `tag`, a `size` and `size` bytes of value per field, the tags ascending. A reader that does not know a tag skips
 * it by its size and keeps going, which is what makes the field addable at all.
 *
 * In a scheme a tagged field is an ordinary `property => type` entry whose type is one of these descriptors, and it
 * is declared **after** the fields of the body, where the wire carries it:
 *
 * <code>
 * return parent::getScheme() + [
 *     'errorCode'              => BinarySchema::TYPE_INT16,
 *     'throttleTimeMs'         => BinarySchema::TYPE_INT32,
 *     'supportedFeatures'      => new TaggedField(0, ['name' => ApiVersionsSupportedFeature::class], []),
 *     'finalizedFeaturesEpoch' => new TaggedField(1, BinarySchema::TYPE_INT64, -1),
 * ];
 * </code>
 *
 * The `type` is any type the engine knows - a scalar, a nested class, an array notation - and the value is written
 * with the encoding of the structure it sits in, which for a tagged field is always the compact one.
 *
 * **The default decides whether the field is written at all.** `Message.write()` of the Java client emits a tagged
 * field only when its value differs from the default of the specification, which is why a `finalized_features_epoch`
 * of `-1` is absent from an answer while a `0` is present. The engine follows that rule strictly (`===`), so a
 * decoded message re-encodes to the bytes it came from.
 *
 * A structure of a flexible version that declares no tagged field at all still ends in the section - as the empty
 * count `00` - and needs no declaration for that: the engine appends it to every structure of a flexible version.
 *
 * @see docs/protocol/2.8.md, sections "Protocol primitive types" and "Implementation model"
 */
final class TaggedField
{
    /**
     * @param int   $tag     Numeric tag of the field, unique inside its structure and never reused (`tag` of the
     *                       JSON message specification)
     * @param mixed $type    Type of the value: a `BinarySchema::TYPE_*` constant, the class name of a nested
     *                       structure, or the array notation of the engine
     * @param mixed $default Value at which the field is left out of the wire, i.e. the `default` of the
     *                       specification: `-1` for an int64 that means "unknown", `[]` for an array, `null` for a
     *                       nullable string, `0`, `false`, `''` for the rest
     */
    public function __construct(
        public readonly int $tag,
        public readonly mixed $type,
        public readonly mixed $default = null
    ) {}
}
