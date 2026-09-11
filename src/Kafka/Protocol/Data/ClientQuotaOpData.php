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
 * One change to one quota of an AlterClientQuotas request (key 49, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   OpData => Key Value Remove
 *     Key    => STRING
 *     Value  => FLOAT64
 *     Remove => BOOLEAN
 * </pre>
 *
 * `Remove` decides what happens: `false` sets the quota to `Value`, `true` deletes it and the broker ignores
 * `Value` - the field is written all the same, because the frame has no optional fields. `AlterClientQuotasRequest`
 * @ 2.8.2 writes `Double.NaN` in that case; this client writes `0.0`, which is what the broker reads and discards.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0 and v1)"
 */
class ClientQuotaOpData implements BinarySchemaInterface
{
    /**
     * Name of the quota to change, one of the KEY_* constants of {@see ClientQuotaValueData}
     */
    public string $key;

    /**
     * New value of the quota, ignored by the broker when {@see self::$remove} is true
     */
    public float $value;

    /**
     * Whether the quota is removed instead of set
     */
    public bool $remove;

    public function __construct(string $key, float $value = 0.0, bool $remove = false)
    {
        $this->key    = $key;
        $this->value  = $value;
        $this->remove = $remove;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'key'    => BinarySchema::TYPE_STRING,
            'value'  => BinarySchema::TYPE_FLOAT64,
            'remove' => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}
