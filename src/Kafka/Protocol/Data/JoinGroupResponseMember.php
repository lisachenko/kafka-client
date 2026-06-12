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
 * Join group request protocol DTO
 *
 *  JoinGroupResponseMember => member_id member_metadata
 *     member_id => STRING
 *     member_metadata => BYTES
 */
class JoinGroupResponseMember implements BinarySchemaInterface
{
    /**
     * Name of the group member
     * @var string
     */
    public $memberId;

    /**
     * Member-specific metadata
     * @var string
     */
    public $metadata;

    public function __construct(string $memberId, string $metadata)
    {
        $this->memberId = $memberId;
        $this->metadata = $metadata;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId' => BinarySchema::TYPE_STRING,
            'metadata' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
