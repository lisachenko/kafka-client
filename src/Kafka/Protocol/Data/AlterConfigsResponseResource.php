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
 * The result of altering one resource, i.e. one entry of the `resources` array of an AlterConfigs answer
 *
 * <pre>
 *   AlterConfigsResponseResource => error_code error_message resource_type resource_name
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 * </pre>
 *
 * `ALTER_CONFIGS_RESPONSE_ENTITY_V0` in `Protocol.java` @ 0.11.0.3 - the entry of {@see DescribeConfigsResponseResource}
 * without the configuration. The error is per resource and carries the message of the exception the broker caught,
 * which is where the reason of a refused value is: the code alone is 42 for everything the `AdminManager` rejects.
 *
 * @see docs/protocol/2.8.md, section "AlterConfigs API (key 33, v0 and v1)"
 */
class AlterConfigsResponseResource implements BinarySchemaInterface
{
    /**
     * Error code of this resource, 0 when its configuration was replaced
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Type of the resource, one of the `ConfigResource::TYPE_*` constants
     */
    public int $resourceType;

    /**
     * Name of the resource: the topic name, or the broker id as a decimal string
     */
    public string $resourceName;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            'resourceType' => BinarySchema::TYPE_INT8,
            'resourceName' => BinarySchema::TYPE_STRING,
        ];
    }
}
