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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Request\AbstractResponse;

/**
 * Response shaped like the ones that wave 2 has not rewritten yet: it parses its payload by hand.
 *
 * It exists to prove that those classes keep working while the branch migrates to schemes.
 */
final class LegacyResponse extends AbstractResponse
{
    public int $errorCode = 0;

    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream): void
    {
        [$self->correlationId, $self->errorCode] = array_values($stream->read('NcorrelationId/nerrorCode'));
    }
}
