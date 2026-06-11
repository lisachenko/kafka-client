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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Basic class for all requests
 *
 * Request Header => api_key api_version correlation_id client_id
 *   api_key => INT16
 *   api_version => INT16
 *   correlation_id => INT32
 *   client_id => NULLABLE_STRING
 */
abstract class AbstractRequest extends AbstractProtocolMessage implements BinarySchemaInterface
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
        $this->messageSize   = BinarySchema::getObjectTypeSize($this) - 4 /* INT32 MessageSize */;
    }

    public static function getScheme()
    {
        return [
            'messageSize'   => BinarySchema::TYPE_INT32,
            'apiKey'        => BinarySchema::TYPE_INT16,
            'apiVersion'    => BinarySchema::TYPE_INT16,
            'correlationId' => BinarySchema::TYPE_INT32,
            'clientId'      => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
