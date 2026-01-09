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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\IO\Stream;

/**
 * ApiVersions response data
 */
class ApiVersionsResponseMetadata
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
     * Unpacks the DTO from the binary buffer
     *
     * @param Stream $stream Binary buffer
     *
     * @return static
     */
    public static function unpack(Stream $stream): static
    {
        $apiVersionMetadata = new static();
        [$apiVersionMetadata->apiKey, $apiVersionMetadata->minVersion, $apiVersionMetadata->maxVersion] = array_values($stream->read('napiKey/nminVersion/nmaxVersion'));

        return $apiVersionMetadata;
    }
}
