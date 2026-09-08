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

namespace Protocol\Kafka\Tests\Common\Errors;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;

final class KafkaExceptionTest extends TestCase
{
    #[DataProvider('codeProvider')]
    public function testFromCodeMapsToTheExpectedExceptionClass(int $code, string $expectedClass): void
    {
        $exception = KafkaException::fromCode($code, []);

        self::assertInstanceOf($expectedClass, $exception);
    }

    public static function codeProvider(): array
    {
        return [
            [KafkaException::CORRUPT_MESSAGE, CorruptMessageException::class],
            [KafkaException::UNKNOWN_TOPIC_OR_PARTITION, UnknownTopicOrPartitionException::class],
            [KafkaException::NOT_LEADER_FOR_PARTITION, NotLeaderForPartitionException::class],
        ];
    }

    public function testUnknownCodeFallsBackToUnknownErrorException(): void
    {
        $exception = KafkaException::fromCode(-999, []);

        self::assertInstanceOf(UnknownErrorException::class, $exception);
    }
}
