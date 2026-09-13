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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One user of a DescribeUserScramCredentials request (key 50, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   UserName => Name
 *     Name => COMPACT_STRING
 * </pre>
 *
 * `DescribeUserScramCredentialsRequest.json` @ 2.8.2 calls the structure `UserName`; it is a structure of one
 * string rather than a bare string array, so in this flexible api every entry carries a tagged-field section of
 * its own.
 *
 * @see docs/protocol/2.8.md, section "DescribeUserScramCredentials API (key 50, v0)"
 */
class ScramUserName implements BinarySchemaInterface
{
    /**
     * Name of the user to describe
     */
    public string $name;

    public function __construct(string $name = '')
    {
        $this->name = $name;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name' => BinarySchema::TYPE_STRING,
        ];
    }
}
