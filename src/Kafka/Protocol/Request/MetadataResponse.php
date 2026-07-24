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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\RestorableTrait;
use Protocol\Kafka\Protocol\BinarySchema;

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
     * The cluster id that this broker belongs to.
     *
     * @since 0.10.1
     *
     * @var string
     */
    public $clusterId;

    /**
     * The broker id of the controller broker.
     *
     * @var integer
     * @since Version 1 of protocol
     */
    public $controllerId;

    /**
     * List of topics
     *
     * @var array|TopicMetadata[]
     */
    public $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'brokers'      => [Node::class],
            'clusterId'    => BinarySchema::TYPE_NULLABLE_STRING,
            'controllerId' => BinarySchema::TYPE_INT32,
            'topics'       => ['topic' => TopicMetadata::class],
        ];
    }
}
