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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\FeatureUpdate;
use Protocol\Kafka\Admin\UpgradeType;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidUpdateVersionException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\Data\FeatureUpdateKey;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesRequest;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesRequestV1;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesResponse;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesResponseV1;

/**
 * Exercises UpdateFeatures (key 57) at the version 2 of Kafka 4.0 and the version 1 of KIP-778 against the 4.3.1 node.
 *
 * The version 1 brought the `upgrade_type` of an update and the top-level `validate_only`, with which the
 * controller says what it *would* do and writes nothing; the version 2 dropped the per-feature results of the
 * answer, because a 4.x controller applies the updates atomically and refuses the whole request at the first
 * feature it cannot change. The finalized features of a node that four agents share are not something a test
 * changes, so every update here is a dry run, or one the controller refuses before it writes, and the levels are
 * read back afterwards.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 to v2)", "The upgrade type and the dry run
 *      of KIP-778 (v1)" and "The answer without results (v2, Kafka 4.0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(UpdateFeaturesRequest::class)]
#[CoversClass(UpdateFeaturesRequestV1::class)]
#[CoversClass(UpdateFeaturesResponse::class)]
#[CoversClass(UpdateFeaturesResponseV1::class)]
#[CoversClass(FeatureUpdateKey::class)]
#[CoversClass(FeatureUpdate::class)]
#[CoversClass(UpgradeType::class)]
final class FeatureUpdateApiTest extends IntegrationTestCase
{
    /**
     * The feature of the node, finalized at the level 30 (`4.3-IV0`) by `kafka-storage.sh format`
     */
    private const string METADATA_VERSION = 'metadata.version';

    private const int FINALIZED_LEVEL = 30;

    /**
     * A feature the node finalizes at the level 1 and may lower to 0: the consumer protocol of KIP-848
     */
    private const string GROUP_VERSION = 'group.version';

    /**
     * The finalized levels of the node, formatted with the defaults of the release; no test may change one
     */
    private const array FINALIZED_FEATURES = [
        'eligible.leader.replicas.version' => 1,
        'group.version'                    => 1,
        'kraft.version'                    => 1,
        'metadata.version'                 => 30,
        'share.version'                    => 1,
        'streams.version'                  => 1,
        'transaction.version'              => 2,
    ];

    /**
     * A feature name no controller knows, unique to this ticket
     */
    private const string UNKNOWN_FEATURE = 't1-40-nonsense';

    /**
     * The sentence a 4.x controller puts in front of the refusal of the one feature it stopped at
     */
    private const string ALL_FEATURES_FAILED = 'The update failed for all features since the following feature had an'
        . ' error: ';

    private const string CLIENT_ID = 'kafka-client-t1-40';

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);

        self::assertSame(
            self::FINALIZED_FEATURES,
            $this->finalizedLevels(),
            'the node has to start this test with the levels its storage was formatted with'
        );
    }

    /**
     * Every test of this class leaves every finalized level where it found it
     */
    protected function tearDown(): void
    {
        self::assertSame(
            self::FINALIZED_FEATURES,
            $this->finalizedLevels(),
            'no test of this class may change a feature of a node four agents share'
        );

        parent::tearDown();
    }

    /**
     * A dry run answers what the controller would do and writes nothing at all
     */
    public function testAValidateOnlyUpdateIsAnsweredWithoutWriting(): void
    {
        $result = $this->admin->updateFeatures(
            [new FeatureUpdate(self::METADATA_VERSION, self::FINALIZED_LEVEL)],
            UpdateFeaturesRequest::DEFAULT_TIMEOUT_MS,
            true
        );

        self::assertSame(
            [self::METADATA_VERSION => null],
            $result,
            'the version 2 answer carries no result, and its top-level 0 is the 0 of every feature of the call'
        );
    }

    /**
     * The upgrade type decides whether a lowering is refused, and a safe one is accepted as a dry run
     *
     * `group.version` is finalized at 1 and may be lowered to 0: an `UPGRADE` of it is refused with the sentence
     * that names the field of KIP-778, the safe and the unsafe downgrade are accepted - and not written, because
     * both are dry runs.
     */
    public function testTheUpgradeTypeDecidesWhetherADowngradeIsAccepted(): void
    {
        $refused = $this->refusalOf([new FeatureUpdate(self::GROUP_VERSION, 0, UpgradeType::Upgrade)], true);

        self::assertInstanceOf(InvalidUpdateVersionException::class, $refused);
        self::assertSame(KafkaException::INVALID_UPDATE_VERSION, $refused->getCode());
        self::assertSame(
            self::ALL_FEATURES_FAILED . 'Invalid update version 0 for feature group.version. Can\'t downgrade the'
            . ' version of this feature without setting the upgrade type to either safe or unsafe downgrade.',
            $refused->getContext()['error'],
            'the refusal names the very field KIP-778 added'
        );

        foreach ([UpgradeType::SafeDowngrade, UpgradeType::UnsafeDowngrade] as $upgradeType) {
            self::assertSame(
                [self::GROUP_VERSION => null],
                $this->admin->updateFeatures(
                    [new FeatureUpdate(self::GROUP_VERSION, 0, $upgradeType)],
                    UpdateFeaturesRequest::DEFAULT_TIMEOUT_MS,
                    true
                ),
                'the controller would lower the level for ' . $upgradeType->name
            );
        }
    }

    /**
     * `metadata.version` 30 cannot be lowered at all, and each upgrade type is refused with its own reason
     */
    public function testTheMetadataVersionOfTheNodeCannotBeLowered(): void
    {
        $lower   = self::FINALIZED_LEVEL - 1;
        $reasons = [
            UpgradeType::Upgrade->name         => 'Invalid update version 29 for feature metadata.version. Can\'t'
                . ' downgrade the version of this feature without setting the upgrade type to either safe or unsafe'
                . ' downgrade.',
            UpgradeType::SafeDowngrade->name   => 'Unsupported metadata.version downgrade from 30 to 29. Refusing to'
                . ' perform the requested downgrade because it might delete metadata information.',
            UpgradeType::UnsafeDowngrade->name => 'Unsupported metadata.version downgrade from 30 to 29. Unsafe'
                . ' metadata downgrade is not supported in this version.',
        ];

        foreach ([UpgradeType::Upgrade, UpgradeType::SafeDowngrade, UpgradeType::UnsafeDowngrade] as $upgradeType) {
            $refused = $this->refusalOf([new FeatureUpdate(self::METADATA_VERSION, $lower, $upgradeType)], true);

            self::assertInstanceOf(InvalidUpdateVersionException::class, $refused);
            self::assertSame(
                self::ALL_FEATURES_FAILED . $reasons[$upgradeType->name],
                $refused->getContext()['error'],
                'the reason of ' . $upgradeType->name
            );
        }

        $tooHigh = $this->refusalOf([new FeatureUpdate(self::METADATA_VERSION, 99)], true);

        self::assertSame(
            self::ALL_FEATURES_FAILED . 'Invalid update version 99 for feature metadata.version. Local controller 1'
            . ' only supports versions 7-30',
            $tooHigh->getContext()['error'],
            'the range of 4.3.1 starts at 3.3-IV3, the level 7'
        );
    }

    /**
     * The byte 0 - the `UNKNOWN` of the Java enum - has a refusal of its own that no version 0 frame can produce
     */
    public function testAnUnknownUpgradeTypeIsRefusedWithItsOwnMessage(): void
    {
        $unknownType = $this->refusalOf([new FeatureUpdate(self::UNKNOWN_FEATURE, 1, UpgradeType::Unknown)]);

        self::assertInstanceOf(InvalidUpdateVersionException::class, $unknownType);
        self::assertSame(
            self::ALL_FEATURES_FAILED . 'Invalid update version 1 for feature ' . self::UNKNOWN_FEATURE . '. The'
            . ' controller does not support the given upgrade type.',
            $unknownType->getContext()['error']
        );

        $unknownFeature = $this->refusalOf([new FeatureUpdate(self::UNKNOWN_FEATURE, 1, UpgradeType::Upgrade)]);

        self::assertSame(
            self::ALL_FEATURES_FAILED . 'Invalid update version 1 for feature ' . self::UNKNOWN_FEATURE . '. Local'
            . ' controller 1 does not support this feature.',
            $unknownFeature->getContext()['error'],
            'a feature the controller does not know is refused before the type is looked at'
        );
    }

    /**
     * The deletion of a feature no controller knows is refused on a 4.x controller (3.9.2 accepted and wrote it)
     */
    public function testTheDeletionOfAnUnknownFeatureIsRefused(): void
    {
        $refused = $this->refusalOf([FeatureUpdate::delete(self::UNKNOWN_FEATURE)]);

        self::assertInstanceOf(InvalidUpdateVersionException::class, $refused);
        self::assertSame(
            self::ALL_FEATURES_FAILED . 'Invalid update version 0 for feature ' . self::UNKNOWN_FEATURE . '. Feature '
            . self::UNKNOWN_FEATURE . ' not found.',
            $refused->getContext()['error'],
            '`Feature.featureFromName` @ 4.0.0 knows the features of the release and nothing else'
        );
    }

    /**
     * One feature the controller refuses refuses the whole request, the features it would accept included
     */
    public function testTheControllerRefusesTheWholeRequestAtTheFirstBadFeature(): void
    {
        $refused = $this->refusalOf(
            [
                new FeatureUpdate(self::METADATA_VERSION, self::FINALIZED_LEVEL),
                new FeatureUpdate(self::UNKNOWN_FEATURE, 1),
            ],
            true
        );

        self::assertInstanceOf(InvalidUpdateVersionException::class, $refused);
        self::assertStringEndsWith(
            'Invalid update version 1 for feature ' . self::UNKNOWN_FEATURE . '. Local controller 1 does not support'
            . ' this feature.',
            (string) $refused->getContext()['error'],
            'the one sentence names the feature it stopped at, and nothing is said about the one it would take'
        );
    }

    /**
     * A version 1 request is still answered per feature when the controller accepts it - with the word `NONE`
     *
     * `QuorumController.updateFeatures` @ 4.0.0 fills the results of a version 0 or 1 answer from
     * `ApiError.NONE`, whose `error().message()` is the name of the code, so the per-feature message of a success
     * is the string `NONE` and not the null of a 3.9.2 controller. A refusal of the same version is the top-level
     * error with an empty result array.
     */
    public function testTheVersionOneAnswerStillCarriesResults(): void
    {
        $stream = $this->connect();
        $update = [self::METADATA_VERSION => new FeatureUpdateKey(self::METADATA_VERSION, self::FINALIZED_LEVEL)];

        new UpdateFeaturesRequestV1($update, 60000, self::CLIENT_ID, 4020, true)->writeTo($stream);
        $accepted = UpdateFeaturesResponseV1::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $accepted->errorCode);
        self::assertNull($accepted->errorMessage);
        self::assertSame([self::METADATA_VERSION], array_keys($accepted->results));
        self::assertSame(KafkaException::NO_ERROR, $accepted->results[self::METADATA_VERSION]->errorCode);
        self::assertSame('NONE', $accepted->results[self::METADATA_VERSION]->errorMessage);

        $unknown = [self::UNKNOWN_FEATURE => new FeatureUpdateKey(self::UNKNOWN_FEATURE, 1)];
        new UpdateFeaturesRequestV1($unknown, 60000, self::CLIENT_ID, 4021, true)->writeTo($stream);
        $refused = UpdateFeaturesResponseV1::unpack($stream);

        self::assertSame(KafkaException::INVALID_UPDATE_VERSION, $refused->errorCode);
        self::assertStringStartsWith(self::ALL_FEATURES_FAILED, (string) $refused->errorMessage);
        self::assertSame([], $refused->results, 'a refusal is the top-level error at every version');

        $stream->disconnect();
    }

    /**
     * An empty update list is not refused, and the version 2 answers it in thirteen bytes
     */
    public function testAnEmptyUpdateListIsAnsweredWithNothing(): void
    {
        self::assertSame([], $this->admin->updateFeatures([]));

        $stream = $this->connect();
        new UpdateFeaturesRequest([], 60000, self::CLIENT_ID, 4022)->writeTo($stream);
        $answer = UpdateFeaturesResponse::unpack($stream);

        self::assertSame(
            '0000000d00000fb6000000000000000000',
            bin2hex((string) $answer),
            'the throttle time, the code 0, a null message and the two tag buffers - no result array at all'
        );

        $stream->disconnect();
    }

    /**
     * Returns the exception the controller refused an update with, or fails when it accepted it
     *
     * @param list<FeatureUpdate> $updates
     */
    private function refusalOf(array $updates, bool $validateOnly = false): KafkaException
    {
        try {
            $this->admin->updateFeatures($updates, UpdateFeaturesRequest::DEFAULT_TIMEOUT_MS, $validateOnly);
        } catch (KafkaException $exception) {
            return $exception;
        }

        self::fail('the controller accepted an update this test expects it to refuse');
    }

    /**
     * Returns the finalized level of every feature, which is the read half of KIP-584 (ApiVersions v4)
     *
     * @return array<string, int>
     */
    private function finalizedLevels(): array
    {
        $levels = [];
        foreach ($this->admin->describeFeatures()->finalizedFeatures as $name => $range) {
            $levels[(string) $name] = $range->maxVersionLevel;
        }
        ksort($levels);

        return $levels;
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
