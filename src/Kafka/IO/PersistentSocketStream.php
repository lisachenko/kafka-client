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
 * @date   26.07.2016
 */

namespace Protocol\Kafka\IO;

use Protocol\Kafka\Common\Errors\NetworkException;

/**
 * PersistentSocketStream allows to keep the connection to sockets between requests to increase performance
 */
class PersistentSocketStream extends SocketStream
{
    /**
     * {@inheritdoc}
     */
    protected function connect()
    {
        $streamSocket = @pfsockopen($this->host, $this->port, $errorNumber, $errorString, $this->timeout);
        if (!$streamSocket) {
            throw new NetworkException(['errorNumber' => $errorNumber, 'errorString' => $errorString]);
        }

        $this->streamSocket = $streamSocket;
    }

    /**
     * @inheritDoc
     */
    protected function disconnect()
    {
        // we don't want to close the connection, so don't call fclose() here
    }
}
