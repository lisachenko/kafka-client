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
 * The result of one acl creation of a CreateAcls answer
 *
 * <pre>
 *   CreateAclsResponseResult => error_code error_message
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING
 * </pre>
 *
 * `AclCreationResult` of `CreateAclsResponse.json` @ 3.3.2. The results come back **in the order of the
 * creations** of the request and there is exactly one per creation - the api has no top-level error code at all,
 * so a request that is refused as a whole carries the same code in every entry.
 *
 * A creation the broker refuses is the error code 42 (`InvalidRequest`) with a message that names what was wrong
 * with it; a refusal of the caller is 31 (`ClusterAuthorizationFailed`), because the api asks the authorizer for
 * `ALTER` on the `CLUSTER` resource.
 *
 * @see docs/protocol/3.9.md, section "CreateAcls API (key 30, v0 to v3)"
 */
class CreateAclsResponseResult implements BinarySchemaInterface
{
    /**
     * Error code of this creation, 0 when the acl was written
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Message of the broker, null when the acl was written
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
