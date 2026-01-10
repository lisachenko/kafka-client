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
 * @date   29.07.2016
 */

namespace Protocol\Kafka\Producer;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 */
class KafkaProducer
{
    /**
     * The producer configs
     */
    private array $configuration;

    /**
     * Kafka cluster configuration
     *
     * @var Cluster
     */
    private $cluster;

    /**
     * Low-level kafka client
     *
     * @var Client
     */
    private $client;

    /**
     * Instance of partitioner
     */
    private readonly PartitionerInterface $partitioner;

    /**
     * Default configuration for producer
     */
    private static array $defaultConfiguration = [
        /* Used configs */
        ProducerConfig::BOOTSTRAP_SERVERS            => [],
        ProducerConfig::PARTITIONER_CLASS            => DefaultPartitioner::class,
        ProducerConfig::ACKS                         => 1,
        ProducerConfig::TIMEOUT_MS                   => 2000,
        ProducerConfig::CLIENT_ID                    => 'PHP/Kafka',
        ProducerConfig::STREAM_PERSISTENT_CONNECTION => false,
        ProducerConfig::STREAM_ASYNC_CONNECT         => false,

        ProducerConfig::KEY_SERIALIZER            => null,
        ProducerConfig::VALUE_SERIALIZER          => null,
        ProducerConfig::BUFFER_MEMORY             => 33554432,
        ProducerConfig::COMPRESSION_TYPE          => 'none',
        ProducerConfig::RETRIES                   => 0,
        ProducerConfig::SSL_KEY_PASSWORD          => null,
        ProducerConfig::SSL_KEYSTORE_LOCATION     => null,
        ProducerConfig::SSL_KEYSTORE_PASSWORD     => null,
        ProducerConfig::BATCH_SIZE                => 0,
        ProducerConfig::CONNECTIONS_MAX_IDLE_MS   => 540000,
        ProducerConfig::LINGER_MS                 => 0,
        ProducerConfig::MAX_REQUEST_SIZE          => 1048576,
        ProducerConfig::RECEIVE_BUFFER_BYTES      => 32768,
        ProducerConfig::REQUEST_TIMEOUT_MS        => 30000,
        ProducerConfig::SASL_MECHANISM            => 'GSSAPI',
        ProducerConfig::SECURITY_PROTOCOL         => 'plaintext',
        ProducerConfig::SEND_BUFFER_BYTES         => 131072,
        ProducerConfig::METADATA_FETCH_TIMEOUT_MS => 60000,
        ProducerConfig::METADATA_MAX_AGE_MS       => 300000,
        ProducerConfig::RECONNECT_BACKOFF_MS      => 50,
        ProducerConfig::RETRY_BACKOFF_MS          => 100,
    ];

    public function __construct(array $configuration = [])
    {
        $this->configuration = ($configuration + self::$defaultConfiguration);
        $this->cluster       = Cluster::bootstrap($this->configuration);
        $partitioner         = $this->configuration[ProducerConfig::PARTITIONER_CLASS];

        if (!is_subclass_of($partitioner, PartitionerInterface::class)) {
            throw new \InvalidArgumentException("Partitioner class should implement PartitionInterface");
        }
        $this->partitioner = new $partitioner();
        $this->client      = new Client($this->cluster, $this->configuration);
    }

    /**
     * Gets the partition metadata for the given topic.
     *
     * @param string $topic
     *
     * @return PartitionMetadata[]
     */
    public function partitionsFor($topic)
    {
        return $this->cluster->partitionsForTopic($topic);
    }

    /**
     * Sends a message to the topic
     *
     * @param string  $topic   Name of the topic
     * @param Record|Record[] $message Record or array of messages to send
     * @param integer|null    $concretePartition Optional partition for sending message
     *
     * @return ProduceResponse
     */
    public function send(string $topic, $message, $concretePartition = null)
    {
        if (isset($concretePartition)) {
            $partition = $concretePartition;
        } else {
            $partition = $this->partitioner->partition($topic, $message->key, $message->value, $this->cluster);
        }

        $topicMessages = ($message instanceof Record) ? [$message] : (array) $message;
        $response      = $this->client->produce($topic, $partition, $topicMessages);

        return $response;
    }
}
