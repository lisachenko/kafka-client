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
 * One topic a Metadata request asks about, the `MetadataRequestTopic` of the specification
 *
 * Up to version 8 the topics of a Metadata request are a plain array of **strings**, and this client writes them
 * as such. `MetadataRequest.json` @ 2.8.2 has always declared them as a `[]MetadataRequestTopic` **structure**
 * with a single `Name` field, and the difference is invisible until version **9**: from the first flexible
 * version of the api on, every structure of the specification ends in a tagged-field section, so a topic entry
 * is a compact string **and a `00`** behind it. A frame that writes the bare strings is one byte short per topic
 * and the broker closes the connection on it - measured against the container, which is why this class exists.
 *
 * Version 10 (KIP-516) adds a `TopicId` in front of the name and makes the name nullable; neither is part of
 * this line.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v9)" and
 *      "Flexible versions in the engine (KIP-482)"
 */
class MetadataRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Metadata API that this DTO belongs to
     */
    public const int VERSION = 9;

    /**
     * Name of the topic to fetch the metadata of
     */
    public string $name = '';

    public function __construct(string $name = '')
    {
        $this->name = $name;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return ['name' => BinarySchema::TYPE_STRING];
    }
}
