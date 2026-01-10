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

/**
 * List groups response
 */
class ListGroupsResponse extends AbstractResponse
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * List of groups as keys and current protocols as values
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
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream): AbstractProtocolMessage
    {
        [$self->correlationId, $self->errorCode, $groupNumber] = array_values($stream->read('NcorrelationId/nerrorCode/NgroupNumber'));

        for ($groupIndex = 0; $groupIndex < $groupNumber; $groupIndex++) {
            $groupId  = $stream->readString();
            $protocol = $stream->readString();

            $self->groups[$groupId] = $protocol;
        }

        return $self;
    }
}
