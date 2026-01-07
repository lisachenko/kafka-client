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
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;

/**
 * Controlled shutdown response
 */
class ControlledShutdownResponse extends AbstractResponse
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * The topic partitions that the broker still leads.
     *
     * @var array|string[]
     */
    public $remainingTopicPartitions = [];

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
        [$self->correlationId, $self->errorCode, $topicPartitionsNumber] = array_values($stream->read('NcorrelationId/nerrorCode/NtopicNumber'));

        for ($i = 0; $i < $topicPartitionsNumber; $i++) {
            $topic     = $stream->readString();
            $partition = $stream->read('Npartition')['partition'];

            $self->remainingTopicPartitions[$topic][] = $partition;
        }

        return $self;
    }
}
