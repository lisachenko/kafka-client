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

/**
 * SASL handshake response
 */
class SaslHandshakeResponse extends AbstractResponse
{
    /**
     * Array of mechanisms enabled in the server.
     *
     * @var array|string[]
     */
    public $enabledMechanisms = [];

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
        [$self->correlationId, $self->errorCode, $mechanismsNumber] = array_values(unpack("NcorrelationId/nerrorCode/NmechanismsNumber", $data));
        $data = substr($data, 10);

        for ($i = 0; $i < $mechanismsNumber; $i++) {
            [$mechanismLength] = array_values(unpack("nmechanismLength", $data));
            $data = substr($data, 2);
            [$mechanism] = array_values(unpack("a{$mechanismLength}mechanism", $data));
            $data = substr($data, $mechanismLength);

            $self->enabledMechanisms[] = $mechanism;
        }

        return $self;
    }
}
