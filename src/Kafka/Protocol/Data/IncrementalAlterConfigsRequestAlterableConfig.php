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

use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One change of an IncrementalAlterConfigs request, i.e. one entry of the `configs` array of a resource
 *
 * <pre>
 *   AlterableConfig => name config_operation value
 *     name             => STRING
 *     config_operation => INT8
 *     value            => NULLABLE_STRING
 * </pre>
 *
 * `AlterableConfig` of `IncrementalAlterConfigsRequest.json` @ 2.8.2. The `config_operation` byte is what makes
 * the api incremental: {@see AlterConfigOp::SET} and {@see AlterConfigOp::DELETE} work on any option,
 * {@see AlterConfigOp::APPEND} and {@see AlterConfigOp::SUBTRACT} only on one whose `ConfigDef.Type` is `LIST`.
 *
 * The **value may only be null for a DELETE**: `ZkAdminManager.incrementalAlterConfigs` @ 2.8.2 collects every
 * other operation with a null value and answers the whole resource with the error code 42 and the message
 * `Null value not supported for : SET:retention.ms`.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0)"
 */
class IncrementalAlterConfigsRequestAlterableConfig implements BinarySchemaInterface
{
    /**
     * Name of the option, e.g. `retention.ms`
     */
    public string $name;

    /**
     * What to do with it, one of the `AlterConfigOp::*` constants
     */
    public int $configOperation;

    /**
     * Value of the option as text, null for a {@see AlterConfigOp::DELETE}
     */
    public ?string $value;

    public function __construct(string $name, int $configOperation = AlterConfigOp::SET, ?string $value = null)
    {
        $this->name            = $name;
        $this->configOperation = $configOperation;
        $this->value           = $value;
    }

    /**
     * Builds the wire entry of a change the caller described with an {@see AlterConfigOp}
     */
    public static function fromAlterConfigOp(AlterConfigOp $operation): self
    {
        return new self($operation->name, $operation->operation, $operation->value);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'            => BinarySchema::TYPE_STRING,
            'configOperation' => BinarySchema::TYPE_INT8,
            'value'           => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
