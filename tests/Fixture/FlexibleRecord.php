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
use Protocol\Kafka\Protocol\PreservesUnknownTaggedFields;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * A structure with one field of every kind the compact encoding of KIP-482 touches.
 *
 * It is deliberately not a message of any api: the point of the flexible encoding is that **the same scheme** is
 * written plainly in one version of an api and compactly in the next one, so the fixture carries no version at all
 * and the test says which encoding it wants. The two tagged fields are declared in descending order of their tag,
 * to show that the wire order is the engine's business and not the scheme's.
 *
 * @see \Protocol\Kafka\Tests\Unit\Protocol\FlexibleSchemaTest
 */
final class FlexibleRecord implements BinarySchemaInterface
{
    use PreservesUnknownTaggedFields;

    public string $name = '';

    public ?string $nickname = null;

    public ?string $payload = null;

    /**
     * @var list<BrokerRecord>
     */
    public array $brokers = [];

    /**
     * Tagged field 3: a list of labels, absent from the wire while it is empty
     *
     * @var list<string>
     */
    public array $labels = [];

    /**
     * Tagged field 1: an epoch whose default -1 keeps it off the wire
     */
    public int $epoch = -1;

    /**
     * @param list<BrokerRecord> $brokers
     * @param list<string>       $labels
     */
    public static function of(
        string $name,
        ?string $nickname = null,
        ?string $payload = null,
        array $brokers = [],
        array $labels = [],
        int $epoch = -1
    ): self {
        $record           = new self();
        $record->name     = $name;
        $record->nickname = $nickname;
        $record->payload  = $payload;
        $record->brokers  = $brokers;
        $record->labels   = $labels;
        $record->epoch    = $epoch;

        return $record;
    }

    public static function getScheme(): array
    {
        return [
            'name'     => BinarySchema::TYPE_STRING,
            'nickname' => BinarySchema::TYPE_NULLABLE_STRING,
            'payload'  => BinarySchema::TYPE_BYTEARRAY,
            'brokers'  => [BrokerRecord::class],
            'labels'   => new TaggedField(3, [BinarySchema::TYPE_STRING], []),
            'epoch'    => new TaggedField(1, BinarySchema::TYPE_INT64, -1),
        ];
    }
}
