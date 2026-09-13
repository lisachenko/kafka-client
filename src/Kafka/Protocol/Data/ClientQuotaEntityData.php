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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One part of a quota entity of the DescribeClientQuotas (48) and AlterClientQuotas (49) apis (Kafka 2.6, KIP-546)
 *
 * <pre>
 *   EntityData => EntityType EntityName
 *     EntityType => STRING
 *     EntityName => NULLABLE_STRING
 * </pre>
 *
 * A quota entity is an **array** of these, because a quota may be attached to a combination: `user` alone,
 * `client-id` alone, `user` *and* `client-id` together, or `ip`. A `null` name is the `<default>` entity of that
 * type - the quota that applies to everybody who has no quota of their own - which is why the field is nullable
 * although `kafka-configs.sh` prints it as the literal string `<default>`.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0 and v1)"
 */
class ClientQuotaEntityData implements BinarySchemaInterface
{
    /**
     * A quota attached to the `client.id` of a connection
     */
    public const string TYPE_CLIENT_ID = 'client-id';

    /**
     * A quota attached to the authenticated principal of a connection
     */
    public const string TYPE_USER = 'user';

    /**
     * A quota attached to the address a connection comes from (the connection-rate quota of KIP-612)
     */
    public const string TYPE_IP = 'ip';

    /**
     * Type of this part of the entity, one of the TYPE_* constants
     */
    public string $entityType;

    /**
     * Name of the entity, null for the `<default>` one of this type
     */
    public ?string $entityName;

    public function __construct(string $entityType, ?string $entityName)
    {
        $this->entityType = $entityType;
        $this->entityName = $entityName;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'entityType' => BinarySchema::TYPE_STRING,
            'entityName' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
