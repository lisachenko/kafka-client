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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\ApiKeys;

/**
 * Basic class for all requests
 */
abstract class AbstractRequest extends AbstractProtocolMessage
{
    /**
     * The version of the API. (INT16)
     *
     * @var integer
     */
    protected $apiVersion = ApiKeys::VERSION;

    /**
     * @param int $apiKey
     * @param int $correlationId
     * @param string $clientId
     */
    public function __construct(/**
     * The id of the request type. (INT16)
     */
        protected $apiKey, /**
     * A user-supplied integer value that will be passed back with the response (INT32)
     */
        protected $correlationId = 0, /**
     * A user specified identifier for the client making the request.
     */
        protected $clientId = ''
    ) {
        $this->setMessageData($this->packPayload());
    }

    /**
     * Method to unpack the payload for the record
     *
     * @param AbstractProtocolMessage|self $self Instance of current frame
     * @param string $data Binary data
     *
     * @return AbstractProtocolMessage
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, $data)
    {
        [$self->apiKey, $self->apiVersion, $self->correlationId, $self->clientId] = array_values(unpack(ApiKeys::REQUEST_HEADER_FORMAT, $data));

        return $self;
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
