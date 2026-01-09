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
use Protocol\Kafka\Protocol\ApiKeys;

/**
 * Basic class for all requests
 */
abstract class AbstractRequest extends AbstractProtocolMessage
{
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
     * @param int $apiVersion
     * @param string $clientId
     */
    public function __construct(/**
     * The id of the request type. (INT16)
     */
        protected $apiKey, /**
     * A user specified identifier for the client making the request.
     */
        protected $clientId = '',
        $correlationId = 0, /**
     * The version of the API. (INT16)
     *
     * @see ApiKeys::VERSION constant value
     */
        protected $apiVersion = ApiKeys::VERSION
    ) {
        $this->correlationId = $correlationId ?: self::$counter++;

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
