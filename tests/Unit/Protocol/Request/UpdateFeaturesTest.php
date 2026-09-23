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

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\FeatureUpdate;
use Protocol\Kafka\Admin\UpgradeType;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\FeatureUpdateKey;
use Protocol\Kafka\Protocol\Data\FeatureUpdateKeyV0;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesRequest;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesRequestV0;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesResponse;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesResponseV0;

/**
 * Byte-exact tests for the version 1 of UpdateFeatures (key 57), the two fields of KIP-778.
 *
 * Kafka 3.3 replaced the `allow_downgrade` boolean of every update with the `upgrade_type` int8 and appended the
 * top-level `validate_only`; the answer did not change at all. The version 0 of both halves lives on in
 * {@see UpdateFeaturesRequestV0} and {@see UpdateFeaturesResponseV0}, and a downgrade type that the version 0
 * cannot express - the unsafe one - is the same frame as the safe one there.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 and v1)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
#[CoversClass(UpdateFeaturesRequest::class)]
#[CoversClass(UpdateFeaturesRequestV0::class)]
#[CoversClass(UpdateFeaturesResponse::class)]
#[CoversClass(UpdateFeaturesResponseV0::class)]
#[CoversClass(FeatureUpdateKey::class)]
#[CoversClass(FeatureUpdateKeyV0::class)]
#[CoversClass(FeatureUpdate::class)]
#[CoversClass(UpgradeType::class)]
final class UpdateFeaturesTest extends TestCase
{
    /**
     * A version 1 request that lowers `metadata.version` to 20 with an unsafe downgrade, as a dry run.
     *
     *   Size            => 00 00 00 2b (43 bytes)
     *   ApiKey          => 00 39 (57), ApiVersion => 00 01
     *   CorrelationId   => 00 00 00 09
     *   ClientId        => 00 04 "test", TAG_BUFFER => 00
     *   TimeoutMs       => 00 00 ea 60 (60000)
     *   FeatureUpdates  => 02 (1 + 1)
     *     Feature         => 11 "metadata.version" (16 + 1)
     *     MaxVersionLevel => 00 14 (20)
     *     UpgradeType     => 03 (UNSAFE_DOWNGRADE), TAG_BUFFER => 00
     *   ValidateOnly    => 01, TAG_BUFFER => 00
     */
    private const string REQUEST_HEX = '0000002b'
        . '0039'
        . '0001'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '0000ea60'
        . '02'
        . '11' . '6d657461646174612e76657273696f6e'
        . '0014'
        . '03'
        . '00'
        . '01'
        . '00';

    public function testTheRequestCarriesTheUpgradeTypeAndTheDryRun(): void
    {
        $request = new UpdateFeaturesRequest(
            ['metadata.version' => new FeatureUpdateKey('metadata.version', 20, UpgradeType::UnsafeDowngrade)],
            60000,
            'test',
            9,
            true
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::UPDATE_FEATURES, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
        self::assertTrue($request->isValidateOnly());
        self::assertSame(60000, $request->getTimeoutMs());
        self::assertSame(
            UpgradeType::UnsafeDowngrade,
            $request->getFeatureUpdates()['metadata.version']->getUpgradeType()
        );
    }

    /**
     * The version 0 frame has the boolean in the place of the byte, and no dry run at all
     */
    public function testTheVersionZeroRequestFoldsTheUpgradeTypeIntoTheBoolean(): void
    {
        $request = new UpdateFeaturesRequestV0(
            ['metadata.version' => new FeatureUpdateKey('metadata.version', 20, UpgradeType::UnsafeDowngrade)],
            60000,
            'test',
            9,
            true
        );

        self::assertSame(
            '0000002a'
            . '0039' . '0000' . '00000009' . '0004' . '74657374' . '00'
            . '0000ea60'
            . '02'
            . '11' . '6d657461646174612e76657273696f6e'
            . '0014'
            . '01'
            . '00'
            . '00',
            bin2hex((string) $request),
            'the byte 03 becomes the boolean 01 and the validate_only never reaches the wire'
        );
        self::assertInstanceOf(FeatureUpdateKeyV0::class, $request->getFeatureUpdates()['metadata.version']);
        self::assertSame(
            UpgradeType::SafeDowngrade,
            $request->getFeatureUpdates()['metadata.version']->getUpgradeType(),
            'a version 0 entry cannot say which downgrade it meant, and the boolean is read as the safe one'
        );
    }

    /**
     * A safe and an unsafe downgrade are one frame below the version 1 and two frames at it
     */
    public function testTheTwoDowngradesAreOneFrameBelowVersionOne(): void
    {
        $safe   = new FeatureUpdateKey('metadata.version', 20, UpgradeType::SafeDowngrade);
        $unsafe = new FeatureUpdateKey('metadata.version', 20, UpgradeType::UnsafeDowngrade);

        $v1 = static fn(FeatureUpdateKey $update): string => bin2hex(
            (string) new UpdateFeaturesRequest(['metadata.version' => $update], 60000, 'test', 9)
        );
        $v0 = static fn(FeatureUpdateKey $update): string => bin2hex(
            (string) new UpdateFeaturesRequestV0(['metadata.version' => $update], 60000, 'test', 9)
        );

        self::assertNotSame($v1($safe), $v1($unsafe), 'the whole point of the byte KIP-778 added');
        self::assertSame($v0($safe), $v0($unsafe), 'and the whole point of replacing the boolean');
    }

    /**
     * The boolean of the deprecated Java constructor is the safe downgrade, and an upgrade is the default
     */
    public function testTheAdminUpdateMapsTheOldBooleanOntoTheUpgradeType(): void
    {
        self::assertSame(UpgradeType::Upgrade, new FeatureUpdate('f', 3)->upgradeType);
        self::assertFalse(new FeatureUpdate('f', 3)->allowDowngrade);
        self::assertSame(UpgradeType::SafeDowngrade, new FeatureUpdate('f', 3, true)->upgradeType);
        self::assertTrue(new FeatureUpdate('f', 3, true)->allowDowngrade);
        self::assertSame(UpgradeType::Upgrade, new FeatureUpdate('f', 3, false)->upgradeType);

        $delete = FeatureUpdate::delete('f');

        self::assertSame(0, $delete->maxVersionLevel, 'anything below 1 means "remove it"');
        self::assertSame(UpgradeType::SafeDowngrade, $delete->upgradeType, 'a removal is a downgrade');
        self::assertSame(
            UpgradeType::UnsafeDowngrade,
            FeatureUpdate::delete('f', UpgradeType::UnsafeDowngrade)->upgradeType
        );
        self::assertSame(
            UpgradeType::UnsafeDowngrade,
            FeatureUpdate::delete('f', UpgradeType::UnsafeDowngrade)->toData()->getUpgradeType(),
            'and the wire entry it builds carries the type the caller named'
        );
    }

    /**
     * The enum is the one of the Java client, and every code it does not define is the `UNKNOWN` of `fromCode`
     */
    public function testTheUpgradeTypeFoldsAnUnknownCodeIntoUnknown(): void
    {
        self::assertSame(1, UpgradeType::Upgrade->value);
        self::assertSame(2, UpgradeType::SafeDowngrade->value);
        self::assertSame(3, UpgradeType::UnsafeDowngrade->value);
        self::assertSame(UpgradeType::Unknown, UpgradeType::fromCode(0));
        self::assertSame(UpgradeType::Unknown, UpgradeType::fromCode(42));
        self::assertSame(UpgradeType::SafeDowngrade, UpgradeType::fromCode(2));
        self::assertFalse(UpgradeType::Upgrade->allowsDowngrade());
        self::assertTrue(UpgradeType::SafeDowngrade->allowsDowngrade());
        self::assertTrue(UpgradeType::UnsafeDowngrade->allowsDowngrade());
        self::assertTrue(
            UpgradeType::Unknown->allowsDowngrade(),
            '`FeatureUpdate.allowDowngrade()` @ 3.3.2 is "anything but an upgrade"'
        );
    }

    /**
     * The answer of the version 1 is the answer of the version 0, field for field
     */
    public function testTheAnswerIsUnchangedByKip778(): void
    {
        $hex = '00000023'
            . '00000009'
            . '00'
            . '00000000'
            . '0000'
            . '01'
            . '02'
            . '11' . '6d657461646174612e76657273696f6e'
            . '0000'
            . '00'
            . '00'
            . '00';

        $response = UpdateFeaturesResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame('', $response->errorMessage, 'the empty string of the KRaft node, not the null of 2.8.2');
        self::assertSame(KafkaException::NO_ERROR, $response->results['metadata.version']->errorCode);
        self::assertNull($response->results['metadata.version']->errorMessage);
        self::assertSame($hex, bin2hex((string) $response));
        self::assertSame(
            UpdateFeaturesResponse::getScheme(),
            UpdateFeaturesResponseV0::getScheme(),
            'the two versions read the very same bytes'
        );
    }
}
