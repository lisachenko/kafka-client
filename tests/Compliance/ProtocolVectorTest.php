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
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Request\AbstractRequest;

/**
 * Replays every documented wire vector of the Kafka 0.9.0.1 protocol through the request and response classes.
 *
 * Each vector is a frame that a Kafka broker really sent or really accepted - 0.9.0.1 for everything the release
 * added, 0.8.2.2 for the version 0 apis whose frames 0.9 does not change - stored as hex in
 * `docs/protocol/vectors/*.json` and shown as an annotated dump in `docs/protocol/0.10.2.md`. For every one of them
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
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function consumerProtocolVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function metadataVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function produceVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function fetchVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function offsetsVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function groupCoordinatorVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function joinGroupVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function syncGroupVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function heartbeatVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function leaveGroupVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function offsetCommitVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function offsetFetchVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function controlledShutdownVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function describeGroupsVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function listGroupsVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function createTopicsVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function deleteTopicsVectors(): iterable
    {
        return VectorFile::provideFor(__FUNCTION__);
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
    #[DataProvider('joinGroupVectors')]
    public function testJoinGroupApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('syncGroupVectors')]
    public function testSyncGroupApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('heartbeatVectors')]
    public function testHeartbeatApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('leaveGroupVectors')]
    public function testLeaveGroupApi(array $vector): void
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
     * @param array<string, mixed> $vector
     */
    #[DataProvider('describeGroupsVectors')]
    public function testDescribeGroupsApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('listGroupsVectors')]
    public function testListGroupsApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('consumerProtocolVectors')]
    public function testConsumerGroupProtocol(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('createTopicsVectors')]
    public function testCreateTopicsApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('deleteTopicsVectors')]
    public function testDeleteTopicsApi(array $vector): void
    {
        $this->assertVectorIsReplayed($vector);
    }

    /**
     * Every vector file has to be replayed by a data provider of this class, so that a new file cannot be forgotten
     *
     * The list of the replayed apis is derived from the providers themselves - a provider is named after the file
     * it reads - instead of being repeated in a constant: a ticket that captures the vectors of a new api adds one
     * provider and one test method of its own and touches nothing another ticket also touches.
     */
    public function testEveryVectorFileIsReplayed(): void
    {
        self::assertSame(
            VectorFile::names(),
            self::providedApis(),
            'A vector file is not covered by a data provider of this class'
        );
    }

    /**
     * Every provider of this class has to be used by a test method, otherwise its vectors are never replayed
     */
    public function testEveryProviderIsUsedByATestMethod(): void
    {
        $used = [];
        foreach (new \ReflectionClass(self::class)->getMethods() as $method) {
            foreach ($method->getAttributes(DataProvider::class) as $attribute) {
                $used[] = $attribute->getArguments()[0];
            }
        }
        sort($used);

        self::assertSame(
            self::providerMethods(),
            $used,
            'A data provider of this class is not used by any test method'
        );
    }

    /**
     * Names of the data provider methods of this class, in alphabetical order
     *
     * @return list<string>
     */
    private static function providerMethods(): array
    {
        $providers = [];
        foreach (new \ReflectionClass(self::class)->getMethods(\ReflectionMethod::IS_STATIC) as $method) {
            if (str_ends_with($method->getName(), 'Vectors')) {
                $providers[] = $method->getName();
            }
        }
        sort($providers);

        return $providers;
    }

    /**
     * Base names of the vector files that the providers of this class read, in alphabetical order
     *
     * @return list<string>
     */
    private static function providedApis(): array
    {
        $apis = array_map(VectorFile::apiOfProvider(...), self::providerMethods());
        sort($apis);

        return $apis;
    }

    /**
     * Decodes a vector, compares its fields and encodes it back
     *
     * @param array<string, mixed> $vector
     */
    private function assertVectorIsReplayed(array $vector): void
    {
        if ($vector['kind'] === 'structure') {
            $this->assertStructureIsReplayed($vector);

            return;
        }

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

    /**
     * Decodes a structure that travels inside a byte array field, compares its fields and encodes it back
     *
     * The payloads of the consumer group protocol are not framed messages of their own: they have no Size field, no
     * header and no api key, and {@see Subscription} and {@see MemberAssignment} read and write them from the plain
     * bytes of a `member_metadata` or `member_assignment` field.
     *
     * @param array<string, mixed> $vector
     */
    private function assertStructureIsReplayed(array $vector): void
    {
        /** @var class-string<MemberAssignment|Subscription> $class */
        $class = $vector['class'];
        $bytes = hex2bin($vector['hex']);
        self::assertIsString($bytes, "Vector {$vector['id']} does not hold valid hex");

        $structure = $class::unpack($bytes);

        self::assertInstanceOf($class, $structure);
        self::assertSame(
            $vector['fields'],
            MessageFields::of($structure),
            "Vector {$vector['id']} decodes into different values than the ones it documents"
        );
        self::assertSame(
            $vector['hex'],
            bin2hex($structure->pack()),
            "Vector {$vector['id']} does not survive a decode and encode round trip"
        );
        self::assertSame(
            $vector['version'],
            $structure->version,
            "Vector {$vector['id']} was recorded with another version of the consumer group protocol"
        );
    }
}
