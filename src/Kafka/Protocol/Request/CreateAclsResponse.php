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
use Protocol\Kafka\Protocol\Data\CreateAclsResponseResult;

/**
 * CreateAcls response object, version 3 (key 30, Kafka 0.11, KIP-140)
 *
 * <pre>
 *   CreateAcls Response (Version: 3) => throttle_time_ms [results]
 *     throttle_time_ms => INT32
 *     results => error_code error_message
 *       error_code    => INT16
 *       error_message => NULLABLE_STRING
 * </pre>
 *
 * **There is no top-level error code.** One result per creation of the request and in its order, so a request
 * that was refused as a whole - a principal that may not `ALTER` the `CLUSTER` resource - carries the same
 * **31** in every entry, and an empty request is answered with an empty array.
 *
 * | Code | Name                        | When                                                              |
 * |------|-----------------------------|---------------------------------------------------------------------|
 * | 0    | None                        | The acl was written, or was already there                           |
 * | 31   | ClusterAuthorizationFailed  | The caller may not `Alter` the `CLUSTER` resource                   |
 * | 42   | InvalidRequest              | The creation is not a concrete acl - a filter value in one of its seven fields |
 * | 54   | SecurityDisabled            | The broker has no `authorizer.class.name` at all                    |
 *
 * @see docs/protocol/4.3.md, section "CreateAcls API (key 30, v0 to v3)"
 */
class CreateAclsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The version 2 of Kafka 2.4 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every creation of the request, in its order
     *
     * @var list<CreateAclsResponseResult>
     */
    public array $results = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'results'        => [CreateAclsResponseResult::class],
        ];
    }
}
