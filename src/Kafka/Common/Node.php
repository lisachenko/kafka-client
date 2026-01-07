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
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\IO\Stream;

/**
 * Information about a ApiKeys node
 */
class Node
{
    /**
     * The broker id.
     *
     * @var integer
     */
    public $nodeId;

    /**
     * The hostname of the broker.
     *
     * @var string
     */
    public $host;

    /**
     * The port on which the broker accepts requests.
     *
     * @var integer
     */
    public $port;

    /**
     * Unpacks the DTO from the binary buffer
     *
     * @param Stream $stream Binary buffer
     *
     * @return static
     */
    public static function unpack(Stream $stream): static
    {
        $brokerMetadata = new static();
        [$brokerMetadata->nodeId, $hostLength] = array_values($stream->read('NnodeId/nhostLength'));
        [$brokerMetadata->host, $brokerMetadata->port] = array_values($stream->read("a{$hostLength}host/Nport"));

        return $brokerMetadata;
    }
}
