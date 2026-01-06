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

use Protocol\Kafka\Common\Record\RecordBatch;
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
     * @param int $requiredAcks
     * @param int $timeout
     */
    public function __construct(private readonly array $topicMessages, private $requiredAcks = 1, private $timeout = 0, $correlationId = 0, $clientId = '')
    {
        parent::__construct(ApiKeys::PRODUCE, $correlationId, $clientId);
    }

    /**
     * @inheritDoc
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
                $messageSetPayload = '';
                foreach ($messages as $message) {
                    $messageSet = RecordBatch::fromMessage($message);
                    $messageSetPayload .= $messageSet;
                }
                $payload .= pack('NN', $partition, strlen($messageSetPayload));
                $payload .= $messageSetPayload;
            }
        }

        return $payload;
    }
}
