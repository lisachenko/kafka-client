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
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;

/**
 * Api versions response
 */
class ApiVersionsResponse extends AbstractResponse
{
    /**
     * API versions supported by the broker.
     *
     * @var array|ApiVersionsResponseMetadata[]
     */
    public $apiVersions = [];

    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Method to unpack the payload for the record
     *
     * @param AbstractProtocolMessage|static $self Instance of current frame
     * @param string $data Binary data
     *
     * @return AbstractProtocolMessage
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, $data): AbstractProtocolMessage
    {
        [$self->correlationId, $self->errorCode, $versionsNumber] = array_values(unpack("NcorrelationId/nerrorCode/NapiVersionsNumber", $data));
        $data = substr($data, 10);

        for ($i = 0; $i < $versionsNumber; $i++) {
            $apiVersionMetadata = new ApiVersionsResponseMetadata();
            [$apiKey, $apiVersionMetadata->minVersion, $apiVersionMetadata->maxVersion] = array_values(unpack("napiKey/nminVersion/nmaxVersion", $data));
            $data = substr($data, 6);

            $self->apiVersions[$apiKey] = $apiVersionMetadata;
        }

        return $self;
    }
}
