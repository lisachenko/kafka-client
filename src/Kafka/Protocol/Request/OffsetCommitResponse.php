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
 * @date 15.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;

/**
 * Offset commit response object
 */
class OffsetCommitResponse extends AbstractResponse
{
    /**
     * List of topics with partition result
     *
     * @var array
     */
    public $topics = [];

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
            $numberOfTopics,
        ] = array_values($stream->read('NcorrelationId/NnumberOfTopics'));

        for ($topic = 0; $topic < $numberOfTopics; $topic++) {
            $topicLength = $stream->read('ntopicLength')['topicLength'];
            [$topicName, $numberOfPartitions] = array_values($stream->read("a{$topicLength}/NnumberOfPartitions"));

            for ($partition = 0; $partition < $numberOfPartitions; $partition++) {
                [$partitionId, $partitionErrorCode] = array_values($stream->read('Npartition/nErrorCode'));
                $self->topics[$topicName][$partitionId] = $partitionErrorCode;
            }
        }

        return $self;
    }
}
