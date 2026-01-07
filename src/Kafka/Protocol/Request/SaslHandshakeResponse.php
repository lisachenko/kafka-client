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

use Protocol\Kafka\IO\Stream;
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
     * @param AbstractProtocolMessage|static $self   Instance of current frame
     * @param Stream $stream Binary data
     *
     * @return AbstractProtocolMessage
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream): AbstractProtocolMessage
    {
        [$self->correlationId, $self->errorCode, $mechanismsNumber] = array_values($stream->read('NcorrelationId/nerrorCode/NmechanismsNumber'));

        for ($i = 0; $i < $mechanismsNumber; $i++) {
            $mechanismLength = $stream->read('nmechanismLength')['mechanismLength'];
            $mechanism       = $stream->read("a{$mechanismLength}mechanism")['mechanism'];

            $self->enabledMechanisms[] = $mechanism;
        }

        return $self;
    }
}
