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

use Protocol\Kafka\Common\AclBinding;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeAclsResponseResource;

/**
 * DescribeAcls response object, version 3 (key 29, Kafka 0.11, KIP-140)
 *
 * <pre>
 *   DescribeAcls Response (Version: 3) => throttle_time_ms error_code error_message [resources]
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => NULLABLE_STRING
 *     resources        => resource_type resource_name pattern_type [acls]
 *       acls => principal host operation permission_type
 * </pre>
 *
 * The answer is grouped by **resource pattern**: one entry per pattern the filter matched, with the acls that
 * were written for it inside it - see {@see DescribeAclsResponseResource}. {@see self::bindings()} flattens the
 * groups into the acls again.
 *
 * There is no per-acl error, only the top-level code:
 *
 * | Code | Name                        | When                                                                    |
 * |------|-----------------------------|---------------------------------------------------------------------------|
 * | 0    | None                        | The filter was applied; `resources` is what it matched, possibly nothing   |
 * | 31   | ClusterAuthorizationFailed  | The caller may not `Describe` the `CLUSTER` resource - the filter is never read |
 * | 42   | InvalidRequest              | A filter the authorizer cannot make sense of, e.g. an unknown pattern type |
 * | 54   | SecurityDisabled            | The broker has no `authorizer.class.name` at all - every line below this one |
 *
 * **The `error_message` of an answer with the code 0 is the empty string, not null.** The generated
 * `DescribeAclsResponseData` @ 3.9.2 initialises the field with `""` and `AclApis.handleDescribeAcls` sets the
 * throttle time and the resources and nothing else, so the field arrives as the compact `01` and never as the
 * null `00` - measured on the node for the foundation of this line.
 *
 * @see docs/protocol/4.3.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class DescribeAclsResponse extends AbstractResponse
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
     * Error code of the request as a whole, 0 when the filter was applied
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Message of the broker; the **empty string** of an answer the node built, not null
     */
    public ?string $errorMessage = null;

    /**
     * Resource patterns the filter matched, with their acls
     *
     * @var list<DescribeAclsResponseResource>
     */
    public array $resources = [];

    /**
     * Returns every acl of the answer as a flat list, i.e. the resource groups undone
     *
     * @return list<AclBinding>
     */
    public function bindings(): array
    {
        $bindings = [];
        foreach ($this->resources as $resource) {
            foreach ($resource->bindings() as $binding) {
                $bindings[] = $binding;
            }
        }

        return $bindings;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'resources'      => [DescribeAclsResponseResource::class],
        ];
    }
}
