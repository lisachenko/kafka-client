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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * ApiVersions response data
 */
class ApiVersionsResponseMetadata implements BinarySchemaInterface
{
    /**
     * Numerical code of API
     *
     * @var integer
     */
    public $apiKey;

    /**
     * Minimum supported version.
     *
     * @var integer
     */
    public $minVersion;

    /**
     * Maximum supported version.
     *
     * @var integer
     */
    public $maxVersion;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'apiKey'     => BinarySchema::TYPE_INT16,
            'minVersion' => BinarySchema::TYPE_INT16,
            'maxVersion' => BinarySchema::TYPE_INT16,
        ];
    }
}
