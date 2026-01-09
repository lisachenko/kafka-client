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
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;

/**
 * Sync group response
 */
class SyncGroupResponse extends AbstractResponse
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Assigned data to the member
     *
     * @var string
     */
    public $memberAssignment;

    /**
     * Method to unpack the payload for the record
     *
     * @param AbstractProtocolMessage|static $self   Instance of current frame
     * @param Stream $stream Binary data
     *
     * SyncGroupResponse => ErrorCode MemberAssignment
     *   ErrorCode => int16
     *   MemberAssignment => bytes
     *
     * @return AbstractProtocolMessage
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream): AbstractProtocolMessage
    {
        [$self->correlationId, $self->errorCode] = array_values($stream->read('NcorrelationId/nerrorCode'));

        $self->memberAssignment = $stream->readByteArray();

        return $self;
    }
}
