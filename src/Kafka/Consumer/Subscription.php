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

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\IO\Stream;

/**
 * A message in kafka is a key-value pair with a small amount of associated metadata.
 */
class Subscription implements \Stringable
{
    /**
     * This is a version id.
     *
     * @var integer
     */
    public $version;

    /**
     * This property holds all the topics for the consumer.
     *
     * @var array
     */
    public $topics;

    /**
     * The UserData field can be used by custom partition assignment strategies.
     *
     * For example, in a sticky partitioning implementation, this field can contain the assignment from the previous
     * generation. In a resource-based assignment strategy, it could include the number of cpus on the machine hosting
     * each consumer instance.
     *
     * @var string
     */
    public $userData;

    public static function fromSubscription(array $topics, $version = 0, $userData = ''): static
    {
        $message = new static();

        $message->topics   = $topics;
        $message->version  = $version;
        $message->userData = $userData;

        return $message;
    }

    /**
     * Unpacks the DTO from the binary buffer
     *
     * @param Stream $stream Binary buffer
     *
     * @return static
     */
    public static function unpack(Stream $stream): static
    {
        $message = new static();

        [$message->version, $topicNumber] = array_values($stream->read('nversion/NtopicNumber'));

        for ($topicIndex = 0; $topicIndex < $topicNumber; $topicIndex++) {
            $message->topics[] = $stream->readString();
        }
        $message->userData = $stream->readByteArray();

        return $message;
    }

    /**
     * @return string
     *
     * ProtocolMetadata => Version Subscription UserData
     *   Version => int16
     *   Subscription => [Topic]
     *     Topic => string
     *   UserData => bytes
     */
    public function __toString(): string
    {
        $payload = pack('nN', $this->version, count($this->topics));
        foreach ($this->topics as $topic) {
            $topicLength = strlen($topic);
            $payload .= pack("na{$topicLength}", $topicLength, $topic);
        }
        $userDataLength = strlen($this->userData);
        $payload .= pack('N', $userDataLength);
        $payload .= $this->userData;

        return $payload;
    }
}
