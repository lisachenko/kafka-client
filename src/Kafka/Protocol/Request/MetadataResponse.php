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

use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Common\RestorableTrait;

/**
 * Metadata response object
 */
class MetadataResponse extends AbstractResponse
{
    use RestorableTrait;

    /**
     * List of broker metadata info
     *
     * @var array|Node[]
     */
    public $brokers = [];

    /**
     * List of topics
     *
     * @var array|TopicMetadata[]
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
        [$self->correlationId, $numberOfBrokers] = array_values($stream->read('NcorrelationId/NnumberOfBrokers'));

        for ($broker = 0; $broker < $numberOfBrokers; $broker++) {
            $brokerNode = Node::unpack($stream);

            $self->brokers[$brokerNode->nodeId] = $brokerNode;
        }
        $numberOfTopics = $stream->read('NnumberOfTopics')['numberOfTopics'];

        for ($topic = 0; $topic < $numberOfTopics; $topic++) {
            $topicMetadata = TopicMetadata::unpack($stream);

            $self->topics[$topicMetadata->topic] = $topicMetadata;
        }
        return $self;
    }
}
