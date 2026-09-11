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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\FeatureUpdate;
use Protocol\Kafka\Admin\ScramMechanism;
use Protocol\Kafka\Admin\UserScramCredentialDeletion;
use Protocol\Kafka\Admin\UserScramCredentialUpsertion;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ScramCredentialInfo;
use Protocol\Kafka\Protocol\Request\AlterUserScramCredentialsRequest;
use Protocol\Kafka\Protocol\Request\AlterUserScramCredentialsResponse;
use Protocol\Kafka\Protocol\Request\DescribeUserScramCredentialsRequest;
use Protocol\Kafka\Protocol\Request\DescribeUserScramCredentialsResponse;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesRequest;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesResponse;

/**
 * Byte-exact tests for the three apis Kafka 2.7 added: the two SCRAM credential apis of KIP-554 (keys 50 and 51)
 * and UpdateFeatures of KIP-584 (key 57).
 *
 * All three are flexible from their version 0 - Kafka 2.7 is well past KIP-482 - so every frame here is a compact
 * one. What makes the pair of KIP-554 interesting is that the **client** does a computation before it sends
 * anything: the salted password of RFC 5802, which is the reason the broker never learns a password.
 *
 * @see docs/protocol/2.8.md, sections "DescribeUserScramCredentials API (key 50, v0)",
 *      "AlterUserScramCredentials API (key 51, v0)" and "UpdateFeatures API (key 57, v0)"
 */
#[CoversClass(DescribeUserScramCredentialsRequest::class)]
#[CoversClass(DescribeUserScramCredentialsResponse::class)]
#[CoversClass(AlterUserScramCredentialsRequest::class)]
#[CoversClass(AlterUserScramCredentialsResponse::class)]
#[CoversClass(UpdateFeaturesRequest::class)]
#[CoversClass(UpdateFeaturesResponse::class)]
#[CoversClass(ScramMechanism::class)]
#[CoversClass(UserScramCredentialUpsertion::class)]
#[CoversClass(UserScramCredentialDeletion::class)]
#[CoversClass(FeatureUpdate::class)]
final class UserScramCredentialsTest extends TestCase
{
    /**
     * DescribeUserScramCredentials request v0 for the single user `alice`.
     *
     *   Size          => 00 00 00 18 (24 bytes)
     *   ApiKey        => 00 32 (50), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 03
     *   ClientId      => 00 04 "test", TAG_BUFFER => 00
     *   Users         => 02             (compact: one user)
     *     Name        => 06 "alice", TAG_BUFFER => 00
     *   TAG_BUFFER    => 00
     */
    private const string DESCRIBE_REQUEST_HEX = '00000018'
        . '0032'
        . '0000'
        . '00000003'
        . '0004' . '74657374'
        . '00'
        . '02'
        . '06' . '616c696365'
        . '00'
        . '00';

    /**
     * The answer: `alice` has a SCRAM-SHA-256 credential of 8192 iterations.
     */
    private const string DESCRIBE_RESPONSE_HEX = '0000001f'
        . '00000003'
        . '00'
        . '00000000'
        . '0000'
        . '00'
        . '02'
        . '06' . '616c696365' . '0000' . '00'
        . '02' . '01' . '00002000' . '00'
        . '00'
        . '00';

    public function testTheDescribeRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DescribeUserScramCredentialsRequest(['alice'], 'test', 3);

        self::assertSame(self::DESCRIBE_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS, $request->getApiKey());
        self::assertTrue(DescribeUserScramCredentialsRequest::isFlexible());
        self::assertSame(DescribeUserScramCredentialsRequest::HEADER_V2, $request->getHeaderVersion());
    }

    /**
     * The null array and the empty one are different bytes and, uniquely in this protocol, the same request
     */
    public function testTheNullUserArrayAndTheEmptyOneAreBothEveryUser(): void
    {
        $everything = bin2hex((string) new DescribeUserScramCredentialsRequest(null, 'test', 3));
        $nothing    = bin2hex((string) new DescribeUserScramCredentialsRequest([], 'test', 3));

        self::assertStringEndsWith('00' . '00' . '00', $everything, 'the compact null array is the byte 00');
        self::assertStringEndsWith('00' . '01' . '00', $nothing, 'and the empty one is 01');
        self::assertNull(new DescribeUserScramCredentialsRequest(null, 'test', 3)->getUsers());
        self::assertSame([], new DescribeUserScramCredentialsRequest([], 'test', 3)->getUsers());
    }

    public function testTheDescribeAnswerNamesTheMechanismAndTheIterations(): void
    {
        $response = DescribeUserScramCredentialsResponse::unpack(
            new StringStream((string) hex2bin(self::DESCRIBE_RESPONSE_HEX))
        );

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(['alice'], array_keys($response->results));

        $credential = $response->results['alice']->credentialInfos[ScramCredentialInfo::MECHANISM_SCRAM_SHA_256];

        self::assertSame(ScramCredentialInfo::MECHANISM_SCRAM_SHA_256, $credential->mechanism);
        self::assertSame(8192, $credential->iterations);
        self::assertSame(self::DESCRIBE_RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The salted password is `Hi()` of RFC 5802, and it is what the request carries instead of the password
     */
    public function testTheUpsertionDerivesTheSaltedPasswordItself(): void
    {
        $upsertion = new UserScramCredentialUpsertion(
            'alice',
            ScramMechanism::ScramSha256,
            'secret',
            8192,
            'a-fixed-salt'
        );

        $data = $upsertion->toData();

        self::assertSame('alice', $data->name);
        self::assertSame(ScramCredentialInfo::MECHANISM_SCRAM_SHA_256, $data->mechanism);
        self::assertSame(8192, $data->iterations);
        self::assertSame('a-fixed-salt', $data->salt);
        self::assertSame(
            hash_pbkdf2('sha256', 'secret', 'a-fixed-salt', 8192, 32, true),
            $data->saltedPassword,
            'PBKDF2-HMAC-SHA256 with the salt and the iteration count of the request, 32 bytes'
        );
        self::assertSame(32, strlen($data->saltedPassword));
        self::assertStringNotContainsString('secret', bin2hex((string) new AlterUserScramCredentialsRequest(
            [],
            [$data],
            'test',
            3
        )), 'the password itself never reaches the wire');
    }

    /**
     * SCRAM-SHA-512 derives a 64-byte key with the other hash, and a salt is generated when none is given
     */
    public function testTheTwoMechanismsDifferInHashAndKeyLength(): void
    {
        self::assertSame('SCRAM-SHA-256', ScramMechanism::ScramSha256->mechanismName());
        self::assertSame('sha256', ScramMechanism::ScramSha256->hashAlgorithm());
        self::assertSame(32, ScramMechanism::ScramSha256->keyLength());
        self::assertSame('SCRAM-SHA-512', ScramMechanism::ScramSha512->mechanismName());
        self::assertSame('sha512', ScramMechanism::ScramSha512->hashAlgorithm());
        self::assertSame(64, ScramMechanism::ScramSha512->keyLength());
        self::assertSame(4096, ScramMechanism::MIN_ITERATIONS);

        $generated = new UserScramCredentialUpsertion('alice', ScramMechanism::ScramSha512, 'secret');

        self::assertSame(24, strlen($generated->salt), 'a salt is generated when the caller gives none');
        self::assertSame(64, strlen($generated->toData()->saltedPassword));
        self::assertSame(ScramMechanism::MIN_ITERATIONS, $generated->iterations);
    }

    public function testAnEmptyUserIsRefusedBeforeItReachesTheBroker(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new UserScramCredentialUpsertion('', ScramMechanism::ScramSha256, 'secret');
    }

    /**
     * A deletion names the user and the mechanism, and the two arrays of the request are separate
     */
    public function testTheDeletionsComeBeforeTheUpsertionsOnTheWire(): void
    {
        $request = new AlterUserScramCredentialsRequest(
            [new UserScramCredentialDeletion('alice', ScramMechanism::ScramSha512)->toData()],
            [new UserScramCredentialUpsertion('alice', ScramMechanism::ScramSha256, 's', 4096, 'salt')->toData()],
            'test',
            3
        );

        $hex = bin2hex((string) $request);

        self::assertStringContainsString(
            '02' . '06' . '616c696365' . '02' . '00' . '02',
            $hex,
            'one deletion of alice with the mechanism 2, then one upsertion'
        );
        self::assertSame(ApiKeys::ALTER_USER_SCRAM_CREDENTIALS, $request->getApiKey());
        self::assertTrue(AlterUserScramCredentialsRequest::isFlexible());
    }

    /**
     * The alter answer has no top-level error code and reports one entry per user
     */
    public function testTheAlterAnswerIsPerUserAndHasNoTopLevelError(): void
    {
        $hex = '00000027'
            . '00000003'
            . '00'
            . '00000000'
            . '02'
            . '06' . '616c696365'
            . '005d'
            . '13' . '546f6f2066657720697465726174696f6e73'
            . '00'
            . '00';

        $response = AlterUserScramCredentialsResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(['alice'], array_keys($response->results));
        self::assertSame(KafkaException::UNACCEPTABLE_CREDENTIAL, $response->results['alice']->errorCode);
        self::assertSame('Too few iterations', $response->results['alice']->errorMessage);
        self::assertSame($hex, bin2hex((string) $response));
        self::assertArrayNotHasKey(
            'errorCode',
            array_diff_key(AlterUserScramCredentialsResponse::getScheme(), ['errorCode' => null]),
            'the answer carries no top-level error code of its own'
        );
    }

    /**
     * A max version level below 1 is the deletion of a finalized feature, and it needs the downgrade flag
     */
    public function testAFeatureUpdateBelowOneIsADeletion(): void
    {
        $delete = FeatureUpdate::delete('metadata.version');

        self::assertSame(0, $delete->maxVersionLevel, 'anything below 1 means "remove it"');
        self::assertTrue($delete->allowDowngrade, 'and a removal is a downgrade, so the flag comes with it');

        $request = new UpdateFeaturesRequest(
            ['metadata.version' => $delete->toData()],
            60000,
            'test',
            3
        );

        self::assertSame(ApiKeys::UPDATE_FEATURES, $request->getApiKey());
        self::assertSame(60000, $request->getTimeoutMs());
        self::assertStringEndsWith(
            '0000' . '01' . '00' . '00',
            bin2hex((string) $request),
            'the version level 0, the downgrade flag and the two tag buffers'
        );
    }

    public function testTheUpdateFeaturesAnswerCarriesBothLevelsOfError(): void
    {
        $hex = '00000020'
            . '00000003'
            . '00'
            . '00000000'
            . '0000'
            . '00'
            . '02'
            . '06' . '616c706861'
            . '002a'
            . '09' . '4e6f742068657265'
            . '00'
            . '00';

        $response = UpdateFeaturesResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'the controller did look at the request');
        self::assertNull($response->errorMessage);
        self::assertSame(KafkaException::INVALID_REQUEST, $response->results['alpha']->errorCode);
        self::assertSame($hex, bin2hex((string) $response));
    }
}
