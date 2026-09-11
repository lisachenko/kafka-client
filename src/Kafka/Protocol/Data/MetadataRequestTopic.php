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

use Protocol\Kafka\Common\Uuid;
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
 * **Version 10 (Kafka 2.8, KIP-516) adds a `TopicId` in front of the name** and makes the name nullable, so that
 * a client can name a topic by its id instead. `MetadataRequest.json` @ 2.8.2 says in the same breath that "this
 * functionality was not implemented on the server. Versions 10 and 11 should not use the topicId field or set
 * topic name to null", so this client writes {@see Uuid::ZERO} and the real name in every entry - which is what
 * the Java client does as well.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v11)", "Topic ids (v10, KIP-516)" and
 *      "Flexible versions in the engine (KIP-482)"
 */
class MetadataRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Metadata API that this DTO belongs to
     */
    public const int VERSION = 10;

    /**
     * Id of the topic to fetch the metadata of, `Uuid::ZERO` for "I am naming it by its name".
     *
     * The server side of the field does not exist in Kafka 2.8.2 - see the note above - so this is the zero uuid
     * in every request this client sends.
     *
     * @since Version 10 of protocol (Kafka 2.8, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Name of the topic to fetch the metadata of
     */
    public string $name = '';

    public function __construct(string $name = '', string $topicId = Uuid::ZERO)
    {
        $this->name    = $name;
        $this->topicId = $topicId;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        // The topic id of KIP-516 sits IN FRONT of the name, which is the field order of the json and therefore
        // the wire order, and the name becomes nullable in the same version
        if (static::VERSION >= 10) {
            return [
                'topicId' => BinarySchema::TYPE_UUID,
                'name'    => BinarySchema::TYPE_NULLABLE_STRING,
            ];
        }

        return ['name' => BinarySchema::TYPE_STRING];
    }
}
