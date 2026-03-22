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
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\RetriableException;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;

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
     * Current iteration of sending data
     */
    private int $currentTry = 0;

    /**
     * Size of the batch
     */
    private int $batchSize = 0;

    /**
     * Buffer for storing topic-partition-messages
     */
    private array $topicPartitionMessages = [];

    public function __construct(array $configuration = [])
    {
        $this->configuration = ($configuration + ProducerConfig::getDefaultConfiguration());
        $this->cluster       = Cluster::bootstrap($this->configuration);
        $partitioner         = $this->configuration[ProducerConfig::PARTITIONER_CLASS];

        if (!is_subclass_of($partitioner, PartitionerInterface::class)) {
            throw new \InvalidArgumentException("Partitioner class should implement PartitionInterface");
        }
        $this->partitioner = new $partitioner();
        $this->client      = new Client($this->cluster, $this->configuration);
    }

    /**
     * Invoking this method makes all buffered records immediately available to send and blocks on the completion of
     * the requests associated with these records.
     */
    public function flush()
    {
        $result           = null;
        $this->currentTry = 0;

        while ($this->currentTry <= $this->configuration[ProducerConfig::RETRIES]) {
            try {
                $result = $this->client->produce($this->topicPartitionMessages);
                // TODO: resolve futures or store result for analysis
                $this->batchSize = 0;

                $this->topicPartitionMessages = [];
                break;
            } catch (NotLeaderForPartitionException) {
                // We just need to reconfigure the cluster, possible current leader is changed
                $this->cluster->reload();
            } catch (RetriableException) {
                $this->cluster->reload();
                $this->currentTry++;
            }
        }

        if ($this->currentTry > $this->configuration[ProducerConfig::RETRIES]) {
            throw new \RuntimeException("Can not deliver messages to the broker");
        }

        return $result;
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
     * @todo Use futures instead of void result
     *
     * @param string       $topic             Name of the topic
     * @param Record       $message           Message to send
     * @param integer|null $concretePartition Optional partition for sending message
     *
     * @return array
     */
    public function send(string $topic, Record $message, $concretePartition = null)
    {
        if (isset($concretePartition)) {
            $partition = $concretePartition;
        } else {
            $partition = $this->partitioner->partition($topic, $message->key, $message->value, $this->cluster);
        }

        $this->topicPartitionMessages[$topic][$partition][] = $message;
        $this->batchSize++;

        if ($this->batchSize < $this->configuration[ProducerConfig::BATCH_SIZE]) {
            // Return nothing, however it would be nice to return a Promise
            return [];
        }

        return $this->flush();
    }

    /**
     * Automatic flushing of all waiting messages, to use async flush, just call fastcgi_finish_request() before
     */
    public function __destruct()
    {
        if ($this->topicPartitionMessages !== []) {
            $this->flush();
        }
    }
}
