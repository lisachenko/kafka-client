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
 * The result of one resource of an IncrementalAlterConfigs answer
 *
 * <pre>
 *   AlterConfigsResourceResponse => error_code error_message resource_type resource_name
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 * </pre>
 *
 * `AlterConfigsResourceResponse` of `IncrementalAlterConfigsResponse.json` @ 2.8.2 - byte for byte the entry of
 * {@see AlterConfigsResponseResource}, and a separate class only because the two apis are separate keys with
 * separate versions.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0)"
 */
class IncrementalAlterConfigsResponseResource implements BinarySchemaInterface
{
    /**
     * Error code of this resource, 0 when its changes were applied
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
