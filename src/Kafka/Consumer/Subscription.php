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
     * This property holds all the topics for the consumer.
     *
     * @var array
     */
    public $topics;

    /**
     * Subscription constructor.
     *
     * @param string[] $topics   List of topics
     * @param int      $version
     * @param string   $userData Additional user data
     */
    public function __construct(array $topics, /**
     * This is a version id.
     */
        public $version = 0, /**
     * The UserData field can be used by custom partition assignment strategies.
     *
     * For example, in a sticky partitioning implementation, this field can contain the assignment from the previous
     * generation. In a resource-based assignment strategy, it could include the number of cpus on the machine hosting
     * each consumer instance.
     */
        public $userData = '')
    {
        $this->topics   = $topics;
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
