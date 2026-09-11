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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResourceV0;

/**
 * DescribeConfigs response object, version 1 (key 32)
 *
 * <pre>
 *   DescribeConfigs Response (Version: 1) => throttle_time_ms [resources]
 *     throttle_time_ms => INT32
 *     resources        => error_code error_message resource_type resource_name [config_entries]
 *       error_code     => INT16
 *       error_message  => NULLABLE_STRING
 *       resource_type  => INT8
 *       resource_name  => STRING
 *       config_entries => config_name config_value read_only config_source is_sensitive [config_synonyms]
 * </pre>
 *
 * One entry per resource of the request, in the order the broker walked its map - which is NOT the order of the
 * request, so a caller matches the entries by type and name and not by position.
 *
 * Version 1 (Kafka 1.1, KIP-226) changed the **config entry** alone: the `is_default` boolean became the
 * `config_source` int8 in the same place and the entry gained its synonyms, see
 * {@see \Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry}. Everything around it - the throttle
 * time, the per-resource error and the resource entry itself - is the frame of version 0
 * ({@see DescribeConfigsResponseV0}).
 *
 * Error codes a 1.1.1 broker reports per resource:
 *
 * | Code | Name                       | Meaning                                                                  |
 * |------|----------------------------|--------------------------------------------------------------------------|
 * | 0    | None                       | The configuration follows                                                |
 * | 17   | InvalidTopic               | The name of a topic resource is not a legal topic name                   |
 * | 29   | TopicAuthorizationFailed   | The client may describe the topic but not its configuration              |
 * | 31   | ClusterAuthorizationFailed | The client may not read a broker resource                                |
 * | 42   | InvalidRequest             | An unknown resource type, or a broker id that is not the one that answers|
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 and v1)"
 */
class DescribeConfigsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Configuration of every requested resource, in the order the broker answered them
     *
     * @var list<DescribeConfigsResponseResource>
     */
    public array $resources = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'resources'      => [static::resourceClass()],
        ];
    }

    /**
     * Returns the class of a resource entry for the version of the API that this response belongs to
     *
     * @return class-string<DescribeConfigsResponseResource>
     */
    protected static function resourceClass(): string
    {
        return static::VERSION >= 1
            ? DescribeConfigsResponseResource::class
            : DescribeConfigsResponseResourceV0::class;
    }
}
