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
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;

/**
 * Group coordinator response
 */
class GroupCoordinatorResponse extends AbstractResponse
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Host and port information for the coordinator for a consumer group.
     *
     * @var GroupCoordinatorResponseMetadata
     */
    public $coordinator;

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
        $coordinatorMetadata = new GroupCoordinatorResponseMetadata();
        [$self->correlationId, $self->errorCode, $coordinatorMetadata->nodeId, $hostLength] = array_values(unpack("NcorrelationId/nerrorCode/NnodeId/nhostLength", $data));
        $data = substr($data, 12);
        [$coordinatorMetadata->host, $coordinatorMetadata->port] = array_values(unpack("a{$hostLength}host/Nport", $data));
        $self->coordinator = $coordinatorMetadata;

        return $self;
    }
}
