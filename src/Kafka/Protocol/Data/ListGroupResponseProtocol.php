<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * ListGroupResponseGroup DTO
 *
 * ListGroupResponseGroup => group_id protocol_type
 *   group_id => STRING
 *   protocol_type => STRING
 */
class ListGroupResponseProtocol implements BinarySchemaInterface
{
    /**
     * The unique group identifier
     *
     * @var string
     */
    public $groupId;

    /**
     * Supported protocol type
     *
     * @var string
     */
    public $protocolType;

    public static function getScheme(): array
    {
        return [
            'groupId'      => BinarySchema::TYPE_STRING,
            'protocolType' => BinarySchema::TYPE_STRING,
        ];
    }
}
