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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\Data\CreateTopicsResponseTopic;

/**
 * What the controller answered for one topic of a {@see AdminClient::createTopics()} call
 *
 * The three fields behind the error are what **KIP-525** added to the answer with the version 5 of the api
 * (Kafka 2.4): the partition count and the replication factor the topic really got - which is what a client that
 * asked for the broker defaults with the -1/-1 of KIP-464 wants to know - and the whole configuration of the new
 * topic, so that the DescribeConfigs a client used to send right afterwards is unnecessary. They are the
 * `numPartitions()`, `replicationFactor()` and `config()` of the Java `CreateTopicsResult`.
 *
 * An answer of a **lower** version carries none of it: {@see $numPartitions} and {@see $replicationFactor} are then
 * {@see CreateTopicsResponseTopic::UNKNOWN} and {@see $config} is `null`. The configuration is null on a version 5
 * answer as well when the broker could not read it back, in which case {@see $configErrorCode} says why - it is the
 * tagged field 0 of the entry, and it is 0 whenever the configuration is there.
 *
 * {@see $topicId} is what **KIP-516** added with the version 7 (Kafka 2.8): the id the controller gave the topic,
 * as the raw 16 bytes of its UUID, which outlive the name - a topic that is deleted and created again under the
 * same name is a different topic and carries a different id. It is
 * {@see CreateTopicsResponseTopic::NO_TOPIC_ID}, sixteen zero bytes, for a topic the controller refused and for
 * every answer below the version 7; `bin2hex()` renders it, and {@see \Protocol\Kafka\Protocol\Data\DeleteTopicsRequestTopic}
 * is where it can be handed back.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v7)"
 */
final class CreatedTopic
{
    /**
     * @param string              $topic             Name of the topic, as the request spelled it
     * @param KafkaException|null $error             Error of this topic, `null` when it was created
     * @param int                 $numPartitions     Partitions the topic got, or `CreateTopicsResponseTopic::UNKNOWN`
     * @param int                 $replicationFactor Replicas per partition, or `CreateTopicsResponseTopic::UNKNOWN`
     * @param Config|null         $config            Configuration of the new topic, `null` when the broker sent none
     * @param int                 $configErrorCode   Why the configuration is null, 0 when there is no reason
     * @param string              $topicId           Id of the new topic, the raw 16 bytes of its UUID
     */
    public function __construct(
        public readonly string $topic,
        public readonly ?KafkaException $error = null,
        public readonly int $numPartitions = CreateTopicsResponseTopic::UNKNOWN,
        public readonly int $replicationFactor = CreateTopicsResponseTopic::UNKNOWN,
        public readonly ?Config $config = null,
        public readonly int $configErrorCode = 0,
        public readonly string $topicId = CreateTopicsResponseTopic::NO_TOPIC_ID
    ) {}

    /**
     * Builds the value object from one entry of a CreateTopics answer
     *
     * @param KafkaException|null $error Error of the entry, already turned into an exception by the caller
     */
    public static function fromResponseTopic(
        string $topic,
        ?CreateTopicsResponseTopic $entry,
        ?KafkaException $error
    ): self {
        if ($entry === null) {
            return new self($topic, $error);
        }

        $config = null;
        if ($entry->configs !== null) {
            $entries = [];
            foreach ($entry->configs as $name => $option) {
                $entries[$name] = new ConfigEntry(
                    $option->name,
                    $option->value,
                    ConfigSource::fromWire($option->configSource),
                    $option->isSensitive,
                    $option->readOnly
                );
            }
            $config = new Config(ConfigResource::topic($topic), $entries);
        }

        return new self(
            $topic,
            $error,
            $entry->numPartitions,
            $entry->replicationFactor,
            $config,
            $entry->topicConfigErrorCode,
            $entry->topicId
        );
    }
}
