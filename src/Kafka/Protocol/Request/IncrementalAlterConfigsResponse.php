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
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsResponseResource;

/**
 * IncrementalAlterConfigs response object, version 0 (key 44)
 *
 * <pre>
 *   IncrementalAlterConfigs Response (Version: 0) => throttle_time_ms [responses]
 *     throttle_time_ms => INT32
 *     responses        => error_code error_message resource_type resource_name
 *       error_code    => INT16
 *       error_message => NULLABLE_STRING
 *       resource_type => INT8
 *       resource_name => STRING
 * </pre>
 *
 * One entry per resource of the request. The error codes are the ones of {@see AlterConfigsResponse} - the two apis
 * share `ZkAdminManager` and the exceptions it catches - with the ones the operations of KIP-339 add:
 *
 * | Code | Name                       | Meaning                                                                    |
 * |------|----------------------------|----------------------------------------------------------------------------|
 * | 0    | None                       | The changes of the resource were applied (or validated)                    |
 * | 29   | TopicAuthorizationFailed   | The client may describe the topic but not alter it                         |
 * | 31   | ClusterAuthorizationFailed | The client may not alter a broker resource                                 |
 * | 40   | InvalidConfig              | An unknown option name in an APPEND or a SUBTRACT, or a value the option refuses |
 * | 42   | InvalidRequest             | An APPEND or a SUBTRACT of an option that is not a list, a null value outside a DELETE, the same option twice, or a broker option that is not dynamic |
 * | 44   | PolicyViolation            | An `alter.config.policy.class.name` on the broker refused the change       |
 *
 * **Kafka 2.4 added the version 1** (KIP-482), the same frame in the flexible encoding.
 * {@see IncrementalAlterConfigsResponseV0} is the one of Kafka 2.3.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0 and v1)"
 */
class IncrementalAlterConfigsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 1;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every requested resource, in the order the broker answered them
     *
     * @var list<IncrementalAlterConfigsResponseResource>
     */
    public array $responses = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'responses'      => [IncrementalAlterConfigsResponseResource::class],
        ];
    }
}
