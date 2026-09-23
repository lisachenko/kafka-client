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
use Protocol\Kafka\Protocol\Request\UpdateFeaturesResponse;

/**
 * Exercises the version 1 of UpdateFeatures (key 57) of KIP-778 against the 3.9.2 KRaft node.
 *
 * Two fields are new: the `upgrade_type` of an update, which replaced the `allow_downgrade` boolean, and the
 * top-level `validate_only`, with which the controller says what it *would* do and writes nothing. The second one
 * is what makes the first measurable at all: the finalized `metadata.version` of a node that four agents share is
 * not something a test lowers, so every downgrade here is a dry run and the level is read back afterwards.
 *
 * The one thing this class really writes is the deletion of a feature the controller has never finalized, which is
 * a `FeatureLevelRecord` with the level 0 - "disabled" - and changes nothing a later test can see.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 and v1)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(UpdateFeaturesRequest::class)]
#[CoversClass(UpdateFeaturesResponse::class)]
#[CoversClass(FeatureUpdateKey::class)]
#[CoversClass(FeatureUpdate::class)]
#[CoversClass(UpgradeType::class)]
final class FeatureUpdateApiTest extends IntegrationTestCase
{
    /**
     * The feature of the node, finalized at the level 21 (`3.9-IV0`) by `kafka-storage.sh format`
     */
    private const string METADATA_VERSION = 'metadata.version';

    private const int FINALIZED_LEVEL = 21;

    /**
     * A feature name no controller knows, unique to this ticket
     */
    private const string UNKNOWN_FEATURE = 't1-33-nonsense';

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);

        self::assertSame(
            self::FINALIZED_LEVEL,
            $this->finalizedMetadataVersion(),
            'the node has to start this test at the level its storage was formatted with'
        );
    }

    /**
     * Every test of this class leaves the finalized level where it found it
     */
    protected function tearDown(): void
    {
        self::assertSame(
            self::FINALIZED_LEVEL,
            $this->finalizedMetadataVersion(),
            'no test of this class may change the metadata version of a node four agents share'
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

        self::assertSame([self::METADATA_VERSION => null], $result, 'the per-feature 0 of an update it accepts');
        self::assertSame(self::FINALIZED_LEVEL, $this->finalizedMetadataVersion());
    }

    /**
     * The three upgrade types are three answers to one and the same lowering
     */
    public function testTheUpgradeTypeDecidesWhetherADowngradeIsAccepted(): void
    {
        $lower = self::FINALIZED_LEVEL - 1;

        $refused = $this->admin->updateFeatures(
            [new FeatureUpdate(self::METADATA_VERSION, $lower, UpgradeType::Upgrade)],
            UpdateFeaturesRequest::DEFAULT_TIMEOUT_MS,
            true
        );

        self::assertInstanceOf(InvalidUpdateVersionException::class, $refused[self::METADATA_VERSION]);
        self::assertSame(
            KafkaException::INVALID_UPDATE_VERSION,
            $refused[self::METADATA_VERSION]->getCode()
        );
        self::assertSame(
            'Invalid update version ' . $lower . ' for feature metadata.version. Can\'t downgrade the version of'
            . ' this feature without setting the upgrade type to either safe or unsafe downgrade.',
            $refused[self::METADATA_VERSION]->getContext()['error'],
            'the refusal names the very field KIP-778 added'
        );

        foreach ([UpgradeType::SafeDowngrade, UpgradeType::UnsafeDowngrade] as $upgradeType) {
            self::assertSame(
                [self::METADATA_VERSION => null],
                $this->admin->updateFeatures(
                    [new FeatureUpdate(self::METADATA_VERSION, $lower, $upgradeType)],
                    UpdateFeaturesRequest::DEFAULT_TIMEOUT_MS,
                    true
                ),
                'the controller would lower the level for ' . $upgradeType->name
            );
        }

        self::assertSame(
            self::FINALIZED_LEVEL,
            $this->finalizedMetadataVersion(),
            'and it wrote none of the three, because every one of them was a dry run'
        );
    }

    /**
     * The byte 0 - the `UNKNOWN` of the Java enum - has a refusal of its own that no version 0 frame can produce
     */
    public function testAnUnknownUpgradeTypeIsRefusedWithItsOwnMessage(): void
    {
        $unknownType = $this->admin->updateFeatures(
            [new FeatureUpdate(self::UNKNOWN_FEATURE, 1, UpgradeType::Unknown)]
        );

        self::assertInstanceOf(InvalidUpdateVersionException::class, $unknownType[self::UNKNOWN_FEATURE]);
        self::assertSame(
            'Invalid update version 1 for feature ' . self::UNKNOWN_FEATURE . '. The controller does not support'
            . ' the given upgrade type.',
            $unknownType[self::UNKNOWN_FEATURE]->getContext()['error']
        );

        $unknownFeature = $this->admin->updateFeatures(
            [new FeatureUpdate(self::UNKNOWN_FEATURE, 1, UpgradeType::Upgrade)]
        );

        self::assertSame(
            'Invalid update version 1 for feature ' . self::UNKNOWN_FEATURE . '. Local controller 1 does not'
            . ' support this feature.',
            $unknownFeature[self::UNKNOWN_FEATURE]->getContext()['error'],
            'a feature the controller does not know is refused before the type is looked at'
        );
    }

    /**
     * The deletion of a feature that was never finalized is accepted, whatever version of the api asks for it
     */
    public function testTheDeletionOfAnUnknownFeatureIsAccepted(): void
    {
        self::assertSame(
            [self::UNKNOWN_FEATURE => null],
            $this->admin->updateFeatures([FeatureUpdate::delete(self::UNKNOWN_FEATURE)]),
            'the level 0 means "disabled", which every node of the quorum supports'
        );
    }

    /**
     * An empty update list is not refused, and its answer carries the empty `error_message` of this node
     */
    public function testAnEmptyUpdateListIsAnsweredWithNothing(): void
    {
        self::assertSame([], $this->admin->updateFeatures([]));
    }

    /**
     * Returns the finalized level of `metadata.version`, which is the read half of KIP-584 (ApiVersions v3)
     */
    private function finalizedMetadataVersion(): int
    {
        $finalized = $this->admin->describeFeatures()->finalizedFeatures[self::METADATA_VERSION] ?? null;

        self::assertNotNull($finalized, 'a KRaft node finalizes its metadata version');

        return $finalized->maxVersionLevel;
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 'kafka-client-t1-33',
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
