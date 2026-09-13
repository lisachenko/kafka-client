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

namespace Protocol\Kafka\Tests\Fixture;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\InlineStruct;

/**
 * A structure that wraps two **flat** fields of its specification in an object of its own.
 *
 * The point of {@see InlineStruct}: `BrokerRecord` is a real structure of the specification in one place - an entry
 * of an array - and a mere group of fields in another, so the marker sits on the *field* and not on the class. In a
 * flexible version the difference is one byte per occurrence: the real structure ends in a tagged-field section and
 * the group does not.
 *
 * @see \Protocol\Kafka\Tests\Unit\Protocol\FlexibleSchemaTest
 */
final class InlinedGroupRecord implements BinarySchemaInterface
{
    public string $name = '';

    /**
     * A group of fields the specification declares flat, which this class reads into an object
     */
    public BrokerRecord $leader;

    /**
     * A real `[]BrokerRecord` of the specification, whose entries are structures
     *
     * @var list<BrokerRecord>
     */
    public array $replicas = [];

    /**
     * @param list<BrokerRecord> $replicas
     */
    public static function of(string $name, BrokerRecord $leader, array $replicas = []): self
    {
        $record           = new self();
        $record->name     = $name;
        $record->leader   = $leader;
        $record->replicas = $replicas;

        return $record;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'     => BinarySchema::TYPE_STRING,
            'leader'   => new InlineStruct(BrokerRecord::class),
            'replicas' => [BrokerRecord::class],
        ];
    }
}
