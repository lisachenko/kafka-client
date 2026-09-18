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

use Protocol\Kafka\Common\AclBinding;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\InlineStruct;

/**
 * One acl that a filter of a DeleteAcls request matched, and whether it could be deleted
 *
 * <pre>
 *   DeleteAclsResponseMatchingAcl => error_code error_message resource_type resource_name pattern_type
 *                                    principal host operation permission_type
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING
 *     ...           => the seven fields of an AclBinding
 * </pre>
 *
 * `DeleteAclsMatchingAcl` of `DeleteAclsResponse.json` @ 3.3.2. A delete is a filter, so the answer has to say
 * **what** it removed: every acl a filter matched is repeated here in full, with its own error code - the deletion
 * of a single acl can fail while the others of the same filter succeed.
 *
 * @see docs/protocol/3.9.md, section "DeleteAcls API (key 31, v0 to v3)"
 */
class DeleteAclsResponseMatchingAcl implements BinarySchemaInterface
{
    /**
     * Error code of the deletion of this acl, 0 when it was removed
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Message of the broker, null when the acl was removed
     */
    public ?string $errorMessage = null;

    /**
     * The acl that was matched, and removed when {@see self::$errorCode} is 0
     */
    public AclBinding $binding;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            // The seven fields of the acl are flat fields of this entry, so they carry no tag buffer of their own
            'binding'      => new InlineStruct(AclBinding::class),
        ];
    }
}
