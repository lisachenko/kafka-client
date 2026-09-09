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
 * Every message and DTO of the protocol describes its binary layout declaratively instead of hand-rolling
 * pack()/unpack() calls.
 *
 * @see BinarySchema for the engine that reads and writes objects described this way
 */
interface BinarySchemaInterface
{
    /**
     * Returns the definition of the binary packet for the class or object.
     *
     * The result maps a property name to its type: one of the BinarySchema::TYPE_* constants, the class name of a
     * nested BinarySchemaInterface object, or an array notation for a repeated structure - `[ItemType]` for a list
     * and `['keyField' => ItemClass::class]` for a list indexed by one of the item fields.
     *
     * @return array<string, mixed>
     */
    public static function getScheme(): array;
}
