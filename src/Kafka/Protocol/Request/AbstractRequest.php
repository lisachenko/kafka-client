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

use Protocol\Kafka\Protocol\AbstractProtocolMessage;

/**
 * Basic class for all requests
 */
abstract class AbstractRequest extends AbstractProtocolMessage
{
    /**
     * Version of API request, could be overridden in children classes
     *
     * @var int
     */
    public const VERSION = 0;

    /**
     * The version of the API. (INT16)
     */
    protected int $apiVersion;

    /**
     * A user-supplied integer value that will be passed back with the response (INT32)
     *
     * @var integer
     */
    protected $correlationId;

    /**
     * Global request counter, ideally this should be stored somewhere in the shared config to survive between requests
     */
    private static int $counter = 0;

    /**
     * @param int $apiKey
     * @param string $clientId
     */
    public function __construct(/**
     * The id of the request type. (INT16)
     */
        protected $apiKey, /**
     * A user specified identifier for the client making the request.
     */
        protected $clientId = '',
        $correlationId = 0
    ) {
        $this->correlationId = $correlationId ?: self::$counter++;
        $this->apiVersion    = static::VERSION;

        $this->setMessageData($this->packPayload());
    }

    /**
     * Implementation of packing the payload
     *
     * @return string
     */
    protected function packPayload()
    {
        $clientLength = strlen($this->clientId);

        return pack(
            "nnNna{$clientLength}",
            $this->apiKey,
            $this->apiVersion,
            $this->correlationId,
            $clientLength,
            $this->clientId
        );
    }
}
