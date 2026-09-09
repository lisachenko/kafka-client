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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Request\AbstractRequest;

/**
 * Metadata request v0 declared the way wave 2 declares every request: a scheme instead of hand-rolled pack() calls.
 *
 * <pre>
 *   TopicMetadataRequest => [TopicName]
 * </pre>
 */
final class SchemaMetadataRequest extends AbstractRequest
{
    public const int API_KEY = ApiKeys::METADATA;

    public const int VERSION = 0;

    /**
     * @param list<string> $topics
     */
    public function __construct(protected array $topics = [], string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => [BinarySchema::TYPE_STRING],
        ];
    }
}
