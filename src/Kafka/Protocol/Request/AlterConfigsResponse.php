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
use Protocol\Kafka\Protocol\Data\AlterConfigsResponseResource;

/**
 * AlterConfigs response object, version 1 (key 33)
 *
 * <pre>
 *   AlterConfigs Response (Version: 0 and 1) => throttle_time_ms [resources]
 *     throttle_time_ms => INT32
 *     resources        => error_code error_message resource_type resource_name
 *       error_code    => INT16
 *       error_message => NULLABLE_STRING
 *       resource_type => INT8
 *       resource_name => STRING
 * </pre>
 *
 * One entry per resource of the request, in the order the broker walked its map. Error codes a 0.11.0.3 broker
 * reports here:
 *
 * | Code | Name                       | Meaning                                                                |
 * |------|----------------------------|------------------------------------------------------------------------|
 * | 0    | None                       | The configuration of the resource was replaced (or validated)          |
 * | 29   | TopicAuthorizationFailed   | The client may describe the topic but not alter it                     |
 * | 31   | ClusterAuthorizationFailed | The client may not alter a broker resource                             |
 * | 40   | InvalidConfig              | An unknown option NAME (`Unknown topic config name: …`)                |
 * | 42   | InvalidRequest             | A resource type other than topic, or an unparsable option VALUE        |
 * | 44   | PolicyViolation            | An `alter.config.policy.class.name` on the broker refused the change   |
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `ALTER_CONFIGS_RESPONSE_V1 =
 * ALTER_CONFIGS_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see AlterConfigsResponseV0} is the same frame with the version field of Kafka 0.11.
 *
 * @see docs/protocol/2.8.md, section "AlterConfigs API (key 33, v0 and v1)"
 */
class AlterConfigsResponse extends AbstractResponse
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
     * Result of every requested resource, in the order the broker answered them
     *
     * @var list<AlterConfigsResponseResource>
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
            'resources'      => [AlterConfigsResponseResource::class],
        ];
    }
}
