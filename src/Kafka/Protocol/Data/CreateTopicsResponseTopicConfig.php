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

use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One configuration option of a topic a CreateTopics **v5** answer reports back (Kafka 2.4, KIP-525)
 *
 * <pre>
 *   CreatableTopicConfigs => name value read_only config_source is_sensitive
 *     name          => COMPACT_STRING
 *     value         => COMPACT_NULLABLE_STRING
 *     read_only     => BOOLEAN
 *     config_source => INT8
 *     is_sensitive  => BOOLEAN
 * </pre>
 *
 * `CreatableTopicConfigs` of `CreateTopicsResponse.json` @ 2.8.2. KIP-525 saves the DescribeConfigs a client used
 * to send right after creating a topic: the answer of the creation already carries the configuration the topic
 * ended up with, entry for entry, including the options the broker inherited from its own settings. The fields are
 * those of a DescribeConfigs entry without the synonyms
 * ({@see DescribeConfigsResponseConfigEntry}), and `config_source` is the same
 * {@see ConfigSource} enumeration.
 *
 * The array itself is **nullable**: `ZkAdminManager.createTopics` @ 2.8.2 leaves it null when it could not read
 * the configuration back, and puts the reason into the tagged `topic_config_error_code` of the topic result.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v6)"
 */
class CreateTopicsResponseTopicConfig implements BinarySchemaInterface
{
    /**
     * Name of the option, e.g. `retention.ms`
     */
    public string $name;

    /**
     * Value of the option as text, null when the option is sensitive
     */
    public ?string $value = null;

    /**
     * Whether the option cannot be changed with the AlterConfigs api
     */
    public bool $readOnly = false;

    /**
     * Where the value comes from, one of the {@see ConfigSource} constants
     */
    public int $configSource = ConfigSource::UNKNOWN;

    /**
     * Whether the option holds a secret, in which case the broker never sends its value
     */
    public bool $isSensitive = false;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'         => BinarySchema::TYPE_STRING,
            'value'        => BinarySchema::TYPE_NULLABLE_STRING,
            'readOnly'     => BinarySchema::TYPE_BOOLEAN,
            'configSource' => BinarySchema::TYPE_INT8,
            'isSensitive'  => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}
