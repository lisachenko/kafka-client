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
use Protocol\Kafka\Protocol\Request\AbstractRequest;

/**
 * Request that uses the typed Stream-based body contract, shaped like a Metadata request v0
 */
final class StubRequest extends AbstractRequest
{
    public const int API_KEY = 3;

    public const int VERSION = 0;

    /**
     * @param list<string> $topics
     */
    public function __construct(private readonly array $topics = [], string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(null, $clientId, $correlationId);
    }

    protected function packBody(Stream $stream): void
    {
        $stream->writeArray($this->topics, static fn(Stream $stream, string $topic) => $stream->writeString($topic));
    }
}
