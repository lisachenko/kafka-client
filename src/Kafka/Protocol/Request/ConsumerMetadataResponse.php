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
use Protocol\Kafka\Protocol\Data\ConsumerMetadataResponseMetadata;

/**
 * Consumer metadata response (ApiKey 10, v0)
 *
 * ConsumerMetadataResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
 *   ErrorCode       => int16
 *   CoordinatorId   => int32
 *   CoordinatorHost => string
 *   CoordinatorPort => int32
 */
class ConsumerMetadataResponse extends AbstractResponse
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Host and port information for the coordinator of a consumer group.
     *
     * @var ConsumerMetadataResponseMetadata
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

        $self->coordinator = ConsumerMetadataResponseMetadata::unpack($stream);

        return $self;
    }
}
