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

use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Protocol\ApiKeys;

/**
 * The produce API
 *
 * The produce API is used to send message sets to the server. For efficiency it allows sending message sets intended
 * for many topic partitions in a single request.
 *
 * The produce API uses the generic message set format, but since no offset has been assigned to the messages at the
 * time of the send the producer is free to fill in that field in any way it likes.
 */
class ProduceRequest extends AbstractRequest
{
    /**
     * ProduceRequest constructor.
     *
     * @param array  $topicMessages List of messages in format: topic => [partition => [messages]]
     * @param int    $requiredAcks  This field indicates how many acknowledgements the servers should receive before
     *                              responding to the request.
     *                              If it is 0 the server will not send any response
     *                              (this is the only case where the server will not reply to a request).
     *                              If it is 1, the server will wait the data is written to the local log before sending
     *                              a response.
     *                              If it is -1 the server will block until the message is committed by all in sync
     *                              replicas before sending a response.
     * @param int    $timeout       This provides a maximum time in milliseconds the server can await the receipt of the
     *                              number of acknowledgements in RequiredAcks.
     * @param string $clientId      ApiKeys client identifier
     * @param int    $correlationId Correlation request ID (will be returned in the response)
     */
    public function __construct(private readonly array $topicMessages, private $requiredAcks = 1, private $timeout = 0, $clientId = '', $correlationId = 0)
    {
        parent::__construct(ApiKeys::PRODUCE, $clientId, $correlationId);
    }

    /**
     * @inheritDoc
     *
     * ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
     *   RequiredAcks => int16
     *   Timeout => int32
     *   Partition => int32
     *   MessageSetSize => int32
     */
    protected function packPayload(): string
    {
        $payload = parent::packPayload();

        $totalTopics = count($this->topicMessages);
        $payload .= pack('nNN', $this->requiredAcks, $this->timeout, $totalTopics);
        foreach ($this->topicMessages as $topic => $partitions) {
            $topicLength = strlen($topic);
            $payload .= pack("na{$topicLength}N", $topicLength, $topic, count($partitions));
            foreach ($partitions as $partition => $messages) {
                $messageSetPayload = MessageSet::fromRecords($messages)->toBuffer();
                $payload .= pack('NN', $partition, strlen($messageSetPayload));
                $payload .= $messageSetPayload;
            }
        }

        return $payload;
    }
}
