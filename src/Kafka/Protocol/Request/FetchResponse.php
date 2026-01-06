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

use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;

/**
 * Fetch response object
 */
class FetchResponse extends AbstractResponse
{
    /**
     * List of fetch responses
     *
     * @var array|FetchResponsePartition[]
     */
    public $topics = [];

    /**
     * Method to unpack the payload for the record
     *
     * @param AbstractProtocolMessage|static $self Instance of current frame
     * @param string $data Binary data
     *
     * @return AbstractProtocolMessage
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, $data): AbstractProtocolMessage
    {
        [
            $self->correlationId,
            $numberOfTopics,
        ] = array_values(unpack("NcorrelationId/NnumberOfTopics", $data));
        $data = substr($data, 8);

        for ($topic = 0; $topic < $numberOfTopics; $topic++) {
            [$topicLength] = array_values(unpack('ntopicLength', $data));
            $data = substr($data, 2);
            [$topicName, $numberOfPartitions] = array_values(unpack("a{$topicLength}/NnumberOfPartitions", $data));
            $data = substr($data, $topicLength + 4);

            for ($partition = 0; $partition < $numberOfPartitions; $partition++) {
                $topicMetadata = self::unpackTopicPartitionInfo($data);
                $self->topics[$topicName][$topicMetadata->partition] = $topicMetadata;
            }

        }

        return $self;
    }

    /**
     * Unpacks the information about topic partition
     *
     * @param string $binaryStreamBuffer Binary buffer
     *
     * @return FetchResponsePartition
     */
    private static function unpackTopicPartitionInfo(string &$binaryStreamBuffer): FetchResponsePartition
    {
        $partition = new FetchResponsePartition();
        [$partition->partition, $partition->errorCode, $partition->highwaterMarkOffset, $messageSetSize] = array_values(unpack("Npartition/nerrorCode/JhighwaterMarkOffset/NmessageSetSize", $binaryStreamBuffer));
        $messageSetBuffer   = substr($binaryStreamBuffer, 18, $messageSetSize);
        $binaryStreamBuffer = substr($binaryStreamBuffer, 18 + $messageSetSize);

        while (!empty($messageSetBuffer)) {
            $messageSet = RecordBatch::unpack($messageSetBuffer);
            $partition->messageSet[] = $messageSet;
        }

        return $partition;
    }
}
