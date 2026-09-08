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
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;

/**
 * Group coordinator response
 *
 * Called ConsumerMetadataResponse in Kafka 0.8.2 (api key 10, v0); the wire format below is unchanged in 0.9.
 *
 * GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
 *   ErrorCode       => int16
 *   CoordinatorId   => int32
 *   CoordinatorHost => string
 *   CoordinatorPort => int32
 */
class GroupCoordinatorResponse extends AbstractResponse
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Host and port information for the coordinator for a consumer group.
     *
     * @var GroupCoordinatorResponseMetadata
     */
    public $coordinator;

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
        [
            $self->correlationId,
            $self->errorCode,
        ] = array_values($stream->read("NcorrelationId/nerrorCode"));

        $self->coordinator = GroupCoordinatorResponseMetadata::unpack($stream);

        return $self;
    }
}
