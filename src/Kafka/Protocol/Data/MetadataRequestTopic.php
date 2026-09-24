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
 * a client can name a topic by its id instead. `MetadataRequest.json` @ 3.1.2 still says in the same breath that
 * "this functionality was not implemented on the server. Versions 10 and 11 should not use the topicId field or
 * set topic name to null", so a version 10 or 11 request of this client writes {@see Uuid::ZERO} and the real
 * name in every entry - which is what the Java client does as well.
 *
 * **Version 12 (Kafka 3.1) is the version at which the server implements it**: the same entry, but the id is now
 * resolved by the broker, so an entry may carry a real id with a `null` name and is answered with the metadata of
 * that topic or with the **100** `UnknownTopicId` of an id the cluster does not host, see
 * {@see \Protocol\Kafka\Protocol\Request\MetadataRequest::byTopicIds()}. The frame of the entry did not
 * change at all between 10 and 12, so this one class writes every one of them.
 *
 * @see docs/protocol/4.3.md, sections "Metadata API (key 3, v0 to v13)", "Topic ids (v10, KIP-516)",
 *      "Metadata by topic id (v12, KIP-516)" and "Flexible versions in the engine (KIP-482)"
 */
class MetadataRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Metadata API that this DTO belongs to
     */
    public const int VERSION = 10;

    /**
     * Builds the entry of a topic that is named by its **id** alone, which Kafka 3.1 serves from version 12 on
     *
     * @param string $topicId The 16 raw bytes of the topic id, {@see \Protocol\Kafka\Common\Uuid}
     */
    public static function byId(string $topicId): static
    {
        return new static(null, $topicId);
    }

    /**
     * Id of the topic to fetch the metadata of, `Uuid::ZERO` for "I am naming it by its name".
     *
     * The server side of the field does not exist below version 12 - see the note above - so this is the zero
     * uuid in every version 10 and 11 request this client sends.
     *
     * @since Version 10 of protocol (Kafka 2.8, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Name of the topic to fetch the metadata of, `null` in an entry that names it by its id alone (version 12)
     */
    public ?string $name = '';

    public function __construct(?string $name = '', string $topicId = Uuid::ZERO)
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
