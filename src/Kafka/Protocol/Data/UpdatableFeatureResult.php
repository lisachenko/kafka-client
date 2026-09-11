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
 * The result of one feature of an UpdateFeatures answer (key 57, Kafka 2.7, KIP-584)
 *
 * <pre>
 *   UpdatableFeatureResult => Feature ErrorCode ErrorMessage
 *     Feature      => COMPACT_STRING
 *     ErrorCode    => INT16
 *     ErrorMessage => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "UpdateFeatures API (key 57, v0)"
 */
class UpdatableFeatureResult implements BinarySchemaInterface
{
    /**
     * Name of the feature this result belongs to
     */
    public string $feature;

    /**
     * Error of this feature, 0 when its version level was changed
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'feature'      => BinarySchema::TYPE_STRING,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
