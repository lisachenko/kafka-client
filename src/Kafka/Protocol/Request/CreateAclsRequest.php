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
use Protocol\Kafka\Protocol\ApiKeys;

/**
 * CreateAcls, version 3: writes acls into the authorizer of the cluster (ApiKey 30, Kafka 0.11, KIP-140)
 *
 * <pre>
 *   CreateAcls Request (Version: 3) => [creations]
 *     creations => resource_type resource_name resource_pattern_type principal host operation permission_type
 *       resource_type         => INT8
 *       resource_name         => STRING
 *       resource_pattern_type => INT8                    -- since version 1 (KIP-290)
 *       principal             => STRING
 *       host                  => STRING
 *       operation             => INT8
 *       permission_type       => INT8
 * </pre>
 *
 * One request writes any number of acls and every one of them is an {@see AclBinding}: a resource pattern and the
 * permission that is written for it. The creations are **not** a transaction - each of them gets its own entry in
 * the answer, in the order of the request, and one refused creation does not undo the others.
 *
 * A creation has to name a concrete pattern: the pattern type is {@see \Protocol\Kafka\Common\PatternType::LITERAL}
 * or {@see \Protocol\Kafka\Common\PatternType::PREFIXED}, the resource type, the operation and the permission type
 * are none of the `ANY`/`UNKNOWN` values a filter may use, and the principal and the host are real strings - the
 * wildcards of a filter are refused here with the error code 42 (`InvalidRequest`). Writing the same acl twice is
 * **not** an error: the authorizer stores a set and answers 0 both times.
 *
 * **The whole request is authorized before a creation is read**: `AclApis.handleCreateAcls` @ 3.9.2 asks the
 * authorizer for `ALTER` on the `CLUSTER` resource, and a principal that may not do it is answered **31** in
 * every entry of the answer. On a KRaft node the write itself is a **controller** write: the broker forwards the
 * request through the envelope api and the acl is visible to a describe once its metadata record has been
 * replayed.
 *
 * **The versions.** Version 1 (Kafka 2.0) added the `resource_pattern_type` of KIP-290, version 2 (Kafka 2.4) is
 * the first **flexible** one and **version 3 (Kafka 3.3) adds the user resource type** of KIP-373 ("Version 3
 * adds user resource type" of `CreateAclsRequest.json` @ 3.3.2) - the version a client has to send to write an
 * acl on a {@see \Protocol\Kafka\Common\ResourceType::USER} resource. No field changed with it.
 *
 * @see docs/protocol/3.9.md, section "CreateAcls API (key 30, v0 to v3)"
 */
class CreateAclsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::CREATE_ACLS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The version 2 of Kafka 2.4 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Acls to write, in the order the answer reports them in
     *
     * @var list<AclBinding>
     */
    protected readonly array $creations;

    /**
     * @param list<AclBinding> $creations     Acls to write; an empty list is answered with an empty result array
     * @param string           $clientId      A user specified identifier for the client making the request
     * @param int              $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(array $creations = [], string $clientId = '', int $correlationId = 0)
    {
        $this->creations = array_values($creations);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the acls the request writes
     *
     * @return list<AclBinding>
     */
    public function getCreations(): array
    {
        return $this->creations;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'creations' => [AclBinding::class],
        ];
    }
}
