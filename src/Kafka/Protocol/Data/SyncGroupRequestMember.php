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

use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * SyncGroupRequest group member assignment
 *
 * GroupAssignment => [MemberId MemberAssignment]
 *   MemberId => string
 *   MemberAssignment => MemberAssignment
 */
class SyncGroupRequestMember implements BinarySchemaInterface
{
    /**
     * Name of the group member
     * @var string
     */
    public $memberId;

    /**
     * Member-specific assignment
     *
     * @var string
     * @todo This field should be MemberAssignment instance in scheme
     */
    public $assignment;

    /**
     * Default initializer
     *
     * @param string $memberId Member identifier
     * @param MemberAssignment $assignment Received assignment
     */
    public function __construct(string $memberId, MemberAssignment $assignment)
    {
        $this->memberId = $memberId;
        // TODO: This should be done on scheme-level
        $stringBuffer = new StringStream();
        BinarySchema::writeObjectToStream($assignment, $stringBuffer);
        $this->assignment = $stringBuffer->getBuffer();
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId'   => BinarySchema::TYPE_STRING,
            'assignment' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
