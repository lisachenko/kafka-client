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

use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;

/**
 * Produce response object
 */
class ProduceResponse extends AbstractResponse
{
    /**
     * List of broker metadata info
     *
     * @var array|ProduceResponsePartition[]
     */
    public $topics;

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
     * @return ProduceResponsePartition
     */
    private static function unpackTopicPartitionInfo(string &$binaryStreamBuffer): ProduceResponsePartition
    {
        $partition = new ProduceResponsePartition();
        [
            $partition->partition,
            $partition->errorCode,
        ] = array_values(unpack("Npartition/nerrorCode", $binaryStreamBuffer));
        $binaryStreamBuffer = substr($binaryStreamBuffer, 6);
        if (PHP_INT_SIZE === 8) {
            $partition->offset = reset(unpack('J', $binaryStreamBuffer));
        } else {
            [, $partition->offset] = array_values(unpack('NlowWord/NhighWord', $binaryStreamBuffer));
        }
        $binaryStreamBuffer = substr($binaryStreamBuffer, 8);

        return $partition;
    }
}
