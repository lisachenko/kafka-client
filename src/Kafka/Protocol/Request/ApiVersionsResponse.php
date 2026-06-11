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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;

/**
 * Api versions response
 */
class ApiVersionsResponse extends AbstractResponse implements BinarySchemaInterface
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * API versions supported by the broker.
     *
     * @var ApiVersionsResponseMetadata[]
     */
    public $apiVersions = [];

    public static function getScheme(): array
    {
        return parent::getScheme() + [
            'errorCode'   => BinarySchema::TYPE_INT16,
            'apiVersions' => ['apiKey' => ApiVersionsResponseMetadata::class],
        ];
    }
}
