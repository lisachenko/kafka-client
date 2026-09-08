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

namespace Protocol\Kafka\Tests\Compliance;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Request\AbstractRequest;

/**
 * Replays every documented wire vector of the Kafka 0.8.2.2 protocol through the request and response classes.
 *
 * Each vector is a frame that a Kafka 0.8.2.2 broker really sent or really accepted, stored as hex in
 * `docs/protocol/vectors/*.json` and shown as an annotated dump in `docs/protocol/0.8.2.md`. For every one of them
 * this suite checks four things:
 *
 * 1. the frame decodes into the class that the vector names;
 * 2. the decoded message carries exactly the field values the vector documents, in the order of the scheme;
 * 3. re-encoding the decoded message reproduces the original frame byte for byte;
 * 4. the announced `Size` matches the length of the frame, and a request announces the api key and version of its
 *    class.
 *
 * There is one data provider per api, so a broken api shows up as a group of failures instead of a single one.
 */
final class ProtocolVectorTest extends TestCase
{
    /**
     * Vector files that the data providers of this class replay, in the order {@see VectorFile::names()} returns them
     */
    private const array REPLAYED_APIS = [
        'controlled-shutdown',
        'fetch',
        'group-coordinator',
        'metadata',
        'offset-commit',
        'offset-fetch',
        'offsets',
        'produce',
    ];

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function metadataVectors(): iterable
    {
        return VectorFile::provide('metadata');
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function produceVectors(): iterable
    {
        return VectorFile::provide('produce');
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function fetchVectors(): iterable
    {
        return VectorFile::provide('fetch');
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function offsetsVectors(): iterable
    {
        return VectorFile::provide('offsets');
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function groupCoordinatorVectors(): iterable
    {
        return VectorFile::provide('group-coordinator');
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function offsetCommitVectors(): iterable
    {
        return VectorFile::provide('offset-commit');
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function offsetFetchVectors(): iterable
    {
        return VectorFile::provide('offset-fetch');
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function controlledShutdownVectors(): iterable
    {
        return VectorFile::provide('controlled-shutdown');
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('metadataVectors')]
    public function testMetadataApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('produceVectors')]
    public function testProduceApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('fetchVectors')]
    public function testFetchApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('offsetsVectors')]
    public function testOffsetsApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('groupCoordinatorVectors')]
    public function testGroupCoordinatorApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('offsetCommitVectors')]
    public function testOffsetCommitApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('offsetFetchVectors')]
    public function testOffsetFetchApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('controlledShutdownVectors')]
    public function testControlledShutdownApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * Every vector file has to be replayed by a data provider of this class, so that a new file cannot be forgotten
     */
    public function testEveryVectorFileIsReplayed(): void
    {
        self::assertSame(
            VectorFile::names(),
            self::REPLAYED_APIS,
            'A vector file is not covered by a data provider of this class'
        );
    }

    /**
     * Decodes a vector, compares its fields and encodes it back
     *
     * @param array<string, mixed> $vector
     */
    private function assertVectorIsReplayed(array $vector): void
    {
        /** @var class-string<AbstractProtocolMessage> $class */
        $class = $vector['class'];
        $frame = hex2bin($vector['hex']);
        self::assertIsString($frame, "Vector {$vector['id']} does not hold valid hex");

        $message = $class::unpack(new StringStream($frame));

        self::assertInstanceOf($class, $message);
        self::assertSame(
            $vector['fields'],
            MessageFields::of($message),
            "Vector {$vector['id']} decodes into different values than the ones it documents"
        );
        self::assertSame(
            $vector['hex'],
            bin2hex((string) $message),
            "Vector {$vector['id']} does not survive a decode and encode round trip"
        );
        self::assertSame(
            strlen($frame) - 4,
            $message->getMessageSize(),
            "The Size field of the vector {$vector['id']} does not match the length of the frame"
        );

        if ($message instanceof AbstractRequest) {
            self::assertSame($vector['apiKey'], $message->getApiKey(), 'The frame carries another api key');
            self::assertSame($vector['version'], $message->getApiVersion());
            // The version constant of the class has to match the version the frame was recorded with, otherwise the
            // vector was replayed through the class of another version of the same api
            self::assertSame($message::VERSION, $message->getApiVersion());
        }
    }
}
