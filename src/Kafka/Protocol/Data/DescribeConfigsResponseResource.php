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
 * The configuration of one resource of a DescribeConfigs answer, i.e. one entry of the `resources` array
 *
 * <pre>
 *   DescribeConfigsResponseResource => error_code error_message resource_type resource_name [config_entries]
 *     error_code     => INT16
 *     error_message  => NULLABLE_STRING
 *     resource_type  => INT8
 *     resource_name  => STRING
 *     config_entries => DescribeConfigsResponseConfigEntry
 * </pre>
 *
 * `DESCRIBE_CONFIGS_RESPONSE_ENTITY_V0` and `…_V1` of `DescribeConfigsResponse.java` @ 1.1.1: the resource entry
 * itself never changed, only the config entries it holds did, which is what the version constant of this DTO picks
 * in {@see self::entryClass()}, see {@see DescribeConfigsResponseResourceV0}.
 *
 * The error is per resource - one bad resource of a request does not spoil the others - and it carries the
 * `error_message` of the exception the broker caught, which is the only place that says WHY a resource was refused.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 and v1)"
 */
class DescribeConfigsResponseResource implements BinarySchemaInterface
{
    /**
     * Version of the DescribeConfigs API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * Error code of this resource, 0 when its configuration follows
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
     * Options of the resource, indexed by the option name; empty for a resource that failed
     *
     * @var array<string, DescribeConfigsResponseConfigEntry>
     */
    public array $configEntries = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'     => BinarySchema::TYPE_INT16,
            'errorMessage'  => BinarySchema::TYPE_NULLABLE_STRING,
            'resourceType'  => BinarySchema::TYPE_INT8,
            'resourceName'  => BinarySchema::TYPE_STRING,
            'configEntries' => ['configName' => static::entryClass()],
        ];
    }

    /**
     * Returns the class of a configuration entry for the version of the API that this DTO belongs to
     *
     * @return class-string<DescribeConfigsResponseConfigEntry>
     */
    protected static function entryClass(): string
    {
        return static::VERSION >= 1
            ? DescribeConfigsResponseConfigEntry::class
            : DescribeConfigsResponseConfigEntryV0::class;
    }
}
