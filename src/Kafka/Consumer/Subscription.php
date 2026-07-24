<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Subscription information that is used for the synchronization between consumers
 *
 * ProtocolMetadata => Version Subscription UserData
 *   Version => int16
 *   Subscription => [Topic]
 *     Topic => string
 *   UserData => bytes
 */
class Subscription implements BinarySchemaInterface
{
    /**
     * This is a version id.
     * @var int
     */
    public $version;

    /**
     * This property holds all the topics for the consumer.
     * @var string[]
     */
    public $topics;

    /**
     * The UserData field can be used by custom partition assignment strategies.
     *
     * For example, in a sticky partitioning implementation, this field can contain the assignment from the previous
     * generation. In a resource-based assignment strategy, it could include the number of cpus on the machine hosting
     * each consumer instance.
     * @var string
     */
    public $userData;

    /**
     * Subscription constructor.
     *
     * @param string[] $topics List of topics
     */
    public function __construct(array $topics, int $version = 0, string $userData = '')
    {
        $this->topics   = $topics;
        $this->version  = $version;
        $this->userData = $userData;
    }

    /**
     * Returns definition of binary packet for the class or object
     *
     * @return array
     */
    public static function getScheme(): array
    {
        return [
            'version'  => BinarySchema::TYPE_INT16,
            'topics'   => [BinarySchema::TYPE_STRING],
            'userData' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
