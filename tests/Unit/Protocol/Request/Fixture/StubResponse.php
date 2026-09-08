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
use Protocol\Kafka\Protocol\Request\AbstractResponse;

/**
 * Minimal response that reads a single error code and topic name after the response header
 */
final class StubResponse extends AbstractResponse
{
    public int $errorCode = 0;

    public ?string $topic = null;

    protected function unpackBody(Stream $stream): void
    {
        $this->errorCode = $stream->readInt16();
        $this->topic     = $stream->readString();
    }
}
