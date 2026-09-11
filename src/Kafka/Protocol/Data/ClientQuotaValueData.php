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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One quota value of a DescribeClientQuotas answer (key 48, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   ValueData => Key Value
 *     Key   => STRING
 *     Value => FLOAT64
 * </pre>
 *
 * `Value` is the **first `float64` of this protocol** - eight bytes of an IEEE 754 double in network order - and
 * it is what makes a quota expressible at all: `producer_byte_rate` is bytes per second, `request_percentage` a
 * percentage of one request-handler thread and `controller_mutation_rate` mutations per second, none of which is
 * a whole number by nature.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0)"
 */
class ClientQuotaValueData implements BinarySchemaInterface
{
    /**
     * Bytes per second a producer of this entity may write (`quota.producer.default` per entity)
     */
    public const string KEY_PRODUCER_BYTE_RATE = 'producer_byte_rate';

    /**
     * Bytes per second a consumer of this entity may read
     */
    public const string KEY_CONSUMER_BYTE_RATE = 'consumer_byte_rate';

    /**
     * Percentage of one request-handler thread this entity may use (KIP-124)
     */
    public const string KEY_REQUEST_PERCENTAGE = 'request_percentage';

    /**
     * Partition mutations per second this entity may ask the controller for (KIP-599, Kafka 2.6)
     */
    public const string KEY_CONTROLLER_MUTATION_RATE = 'controller_mutation_rate';

    /**
     * Name of the quota, one of the KEY_* constants
     */
    public string $key;

    /**
     * Value of the quota
     */
    public float $value;

    public function __construct(string $key, float $value)
    {
        $this->key   = $key;
        $this->value = $value;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'key'   => BinarySchema::TYPE_STRING,
            'value' => BinarySchema::TYPE_FLOAT64,
        ];
    }
}
