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

namespace Protocol\Kafka\Tests\Unit\Producer\Internals;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\Internals\CompressingClient;
use Protocol\Kafka\Producer\ProducerConfig;

/**
 * Verifies that the batches of a producer with a `compression.type` are compressed as a whole.
 *
 * A compressed batch is a message set of exactly one message, whose attributes announce the codec and whose value is
 * the whole batch; this is what the broker stores and what a consumer unwraps again.
 */
#[CoversClass(CompressingClient::class)]
final class CompressingClientTest extends TestCase
{
    /**
     * Codec of every supported value of the `compression.type` option
     *
     * @return \Generator<string, array{0: string, 1: int}>
     */
    public static function compressionTypes(): \Generator
    {
        yield 'gzip'   => [ProducerConfig::COMPRESSION_TYPE_GZIP, CompressionCodec::GZIP];
        yield 'snappy' => [ProducerConfig::COMPRESSION_TYPE_SNAPPY, CompressionCodec::SNAPPY];
    }

    #[DataProvider('compressionTypes')]
    public function testTheWholeBatchOfAPartitionBecomesOneCompressedMessage(
        string $compressionType,
        int $expectedCodec
    ): void {
        $records = [
            Record::fromKeyValue('key-0', str_repeat('a repetitive value ', 32)),
            Record::fromKeyValue('key-1', str_repeat('a repetitive value ', 32)),
            Record::fromValue(str_repeat('a repetitive value ', 32)),
        ];

        $messageSets = CompressingClient::buildMessageSets(
            ['a-topic' => [2 => $records]],
            ProducerConfig::compressionCodec($compressionType)
        );

        $buffer = $messageSets['a-topic'][2]->toBuffer();

        // The set that goes over the wire holds the single wrapper message of the batch
        $wrapper = self::firstMessageOf($buffer);
        self::assertTrue($wrapper->isCompressed());
        self::assertSame($expectedCodec, $wrapper->getCompressionCodec());
        self::assertLessThan(MessageSet::fromRecords($records)->sizeInBytes(), strlen($buffer));

        // ... and unwrapping it gives the records back, exactly as the broker and a consumer see them
        $storedRecords = MessageSet::fromBuffer($buffer)->getRecords();
        self::assertCount(3, $storedRecords);
        self::assertSame(['key-0', 'key-1', null], array_column($storedRecords, 'key'));
        self::assertSame([0, 1, 2], array_column($storedRecords, 'offset'));
        self::assertSame($records[0]->value, $storedRecords[0]->value);
    }

    public function testAnUncompressedBatchIsLeftAsItIs(): void
    {
        $messageSets = CompressingClient::buildMessageSets(
            ['a-topic' => [0 => [Record::fromValue('a value')]]],
            CompressionCodec::NONE
        );

        self::assertFalse(self::firstMessageOf($messageSets['a-topic'][0]->toBuffer())->isCompressed());
    }

    public function testEveryTopicPartitionOfTheRequestIsCompressedOnItsOwn(): void
    {
        $messageSets = CompressingClient::buildMessageSets(
            [
                'first-topic'  => [0 => [Record::fromValue('a')], 1 => [Record::fromValue('b')]],
                'second-topic' => [7 => [Record::fromValue('c')]],
            ],
            CompressionCodec::GZIP
        );

        self::assertSame(['first-topic', 'second-topic'], array_keys($messageSets));
        self::assertSame([0, 1], array_keys($messageSets['first-topic']));
        self::assertSame([7], array_keys($messageSets['second-topic']));

        foreach ($messageSets as $partitionMessageSets) {
            foreach ($partitionMessageSets as $messageSet) {
                self::assertCount(1, $messageSet, 'A compressed batch is a single wrapper message');
                self::assertTrue(self::firstMessageOf($messageSet->toBuffer())->isCompressed());
            }
        }
    }

    /**
     * Reads the first message of a serialized message set, without unwrapping a compressed one
     */
    private static function firstMessageOf(string $buffer): Message
    {
        /** @var array{messageSize: int} $header */
        $header = unpack('Joffset/NmessageSize', $buffer);

        return Message::fromBuffer(substr($buffer, MessageSet::ENTRY_OVERHEAD, $header['messageSize']));
    }
}
