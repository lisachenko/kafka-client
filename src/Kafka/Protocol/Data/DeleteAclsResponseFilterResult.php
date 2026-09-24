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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of one filter of a DeleteAcls answer: its error, and every acl it removed
 *
 * <pre>
 *   DeleteAclsResponseFilterResult => error_code error_message [matching_acls]
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING
 *     matching_acls => DeleteAclsResponseMatchingAcl
 * </pre>
 *
 * `DeleteAclsFilterResult` of `DeleteAclsResponse.json` @ 3.3.2, one per filter of the request and in its order.
 * A filter that matched nothing is **not** an error: the code is 0 and the array is empty, which is the answer of
 * a delete of an acl that was not there.
 *
 * @see docs/protocol/4.3.md, section "DeleteAcls API (key 31, v0 to v3)"
 */
class DeleteAclsResponseFilterResult implements BinarySchemaInterface
{
    /**
     * Error code of this filter, 0 when it was applied - whether or not it matched anything
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Message of the broker, null when the filter was applied
     */
    public ?string $errorMessage = null;

    /**
     * Every acl this filter matched, with the error code of its deletion
     *
     * @var list<DeleteAclsResponseMatchingAcl>
     */
    public array $matchingAcls = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            'matchingAcls' => [DeleteAclsResponseMatchingAcl::class],
        ];
    }
}
