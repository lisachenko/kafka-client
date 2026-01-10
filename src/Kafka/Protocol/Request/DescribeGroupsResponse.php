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
/**
 * @author Alexander.Lisachenko
 * @date 28.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;

/**
 * Describe groups response
 */
class DescribeGroupsResponse extends AbstractResponse
{
    /**
     * List of groups as keys and group info as values
     *
     * @var array
     */
    public $groups = [];

    /**
     * Method to unpack the payload for the record
     *
     * @param AbstractProtocolMessage|static $self   Instance of current frame
     * @param Stream $stream Binary data
     *
     * @return AbstractProtocolMessage
     *
     * DescribeGroupsResponse => [ErrorCode GroupId State ProtocolType Protocol Members]
     *   ErrorCode => int16
     *   GroupId => string
     *   State => string
     *   ProtocolType => string
     *   Protocol => string
     *   Members => [MemberId ClientId ClientHost MemberMetadata MemberAssignment]
     *     MemberId => string
     *     ClientId => string
     *     ClientHost => string
     *     MemberMetadata => bytes
     *     MemberAssignment => bytes

     */
    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream): AbstractProtocolMessage
    {
        [$self->correlationId, $groupNumber] = array_values($stream->read('NcorrelationId/NgroupNumber'));

        for ($groupIndex = 0; $groupIndex < $groupNumber; $groupIndex++) {
            $groupMetadata = DescribeGroupResponseMetadata::unpack($stream);
            $self->groups[$groupMetadata->groupId] = $groupMetadata;
        }

        return $self;
    }
}
