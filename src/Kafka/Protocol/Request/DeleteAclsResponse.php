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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DeleteAclsResponseFilterResult;

/**
 * DeleteAcls response object, version 3 (key 31, Kafka 0.11, KIP-140)
 *
 * <pre>
 *   DeleteAcls Response (Version: 3) => throttle_time_ms [filter_results]
 *     throttle_time_ms => INT32
 *     filter_results => error_code error_message [matching_acls]
 *       matching_acls => error_code error_message resource_type resource_name pattern_type
 *                        principal host operation permission_type
 * </pre>
 *
 * **There is no top-level error code**, one result per filter of the request and in its order, and every acl a
 * filter matched is repeated in full inside it - with an error code of its own, because a single deletion can
 * fail while the rest of the same filter succeeds.
 *
 * A filter that matched **nothing** is not an error: the code is 0 and the array of matching acls is empty, which
 * is what a delete of an acl that was never written looks like. A principal that may not `Alter` the `CLUSTER`
 * resource is answered **31** in every filter result, with no matching acl.
 *
 * @see docs/protocol/3.9.md, section "DeleteAcls API (key 31, v0 to v3)"
 */
class DeleteAclsResponse extends AbstractResponse
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
     * Result of every filter of the request, in its order
     *
     * @var list<DeleteAclsResponseFilterResult>
     */
    public array $filterResults = [];

    /**
     * Returns every acl the request removed, i.e. the matching acls of every filter that carry the code 0
     *
     * @return list<AclBinding>
     */
    public function deletedBindings(): array
    {
        $deleted = [];
        foreach ($this->filterResults as $result) {
            foreach ($result->matchingAcls as $matching) {
                if ($matching->errorCode === 0) {
                    $deleted[] = $matching->binding;
                }
            }
        }

        return $deleted;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'filterResults'  => [DeleteAclsResponseFilterResult::class],
        ];
    }
}
