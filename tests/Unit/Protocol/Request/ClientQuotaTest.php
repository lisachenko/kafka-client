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
use Protocol\Kafka\Admin\ClientQuotaAlteration;
use Protocol\Kafka\Admin\ClientQuotaAlterationOp;
use Protocol\Kafka\Admin\ClientQuotaEntity;
use Protocol\Kafka\Admin\ClientQuotaFilter;
use Protocol\Kafka\Admin\ClientQuotaFilterComponent;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ClientQuotaComponentData;
use Protocol\Kafka\Protocol\Data\ClientQuotaEntityData;
use Protocol\Kafka\Protocol\Data\ClientQuotaOpData;
use Protocol\Kafka\Protocol\Data\ClientQuotaValueData;
use Protocol\Kafka\Protocol\Request\AlterClientQuotasRequest;
use Protocol\Kafka\Protocol\Request\AlterClientQuotasRequestV0;
use Protocol\Kafka\Protocol\Request\AlterClientQuotasResponse;
use Protocol\Kafka\Protocol\Request\DescribeClientQuotasRequest;
use Protocol\Kafka\Protocol\Request\DescribeClientQuotasRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeClientQuotasResponse;

/**
 * Byte-exact tests for the two client-quota apis of KIP-546 (keys 48 and 49, Kafka 2.6).
 *
 * They are the only apis of this protocol that carry a **`float64`** - eight bytes of an IEEE 754 double in
 * network order - and the only pair of this line that this client speaks in **both** encodings: the plain version
 * 0 that Kafka 2.6 added and the flexible version 1 of Kafka 2.8, which is the same frame with compact types and
 * tagged-field sections.
 *
 * @see docs/protocol/2.8.md, sections "DescribeClientQuotas API (key 48, v0 and v1)" and
 *      "AlterClientQuotas API (key 49, v0 and v1)"
 */
#[CoversClass(DescribeClientQuotasRequest::class)]
#[CoversClass(DescribeClientQuotasResponse::class)]
#[CoversClass(AlterClientQuotasRequest::class)]
#[CoversClass(AlterClientQuotasResponse::class)]
#[CoversClass(ClientQuotaComponentData::class)]
#[CoversClass(ClientQuotaEntityData::class)]
#[CoversClass(ClientQuotaValueData::class)]
#[CoversClass(ClientQuotaOpData::class)]
#[CoversClass(ClientQuotaEntity::class)]
#[CoversClass(ClientQuotaFilter::class)]
#[CoversClass(ClientQuotaFilterComponent::class)]
#[CoversClass(ClientQuotaAlteration::class)]
#[CoversClass(ClientQuotaAlterationOp::class)]
final class ClientQuotaTest extends TestCase
{
    /**
     * DescribeClientQuotas request v0 for the exact `client-id` `events`, non-strict.
     *
     *   Size          => 00 00 00 27 (39 bytes)
     *   ApiKey        => 00 30 (48), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test"
     *   Components    => 00 00 00 01
     *     EntityType => 00 09 "client-id"
     *     MatchType  => 00
     *     Match      => 00 06 "events"
     *   Strict => 00
     */
    private const string DESCRIBE_REQUEST_V0_HEX = '00000027'
        . '0030'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00000001'
        . '0009' . '636c69656e742d6964'
        . '00'
        . '0006' . '6576656e7473'
        . '00';

    /**
     * The very same filter in the flexible version 1, which is nine bytes shorter.
     *
     *   ApiVersion => 00 01, then the tag buffer of the request header v2
     *   Components => 02                      (compact: one component)
     *     EntityType => 0a "client-id", MatchType => 00, Match => 07 "events", TAG_BUFFER => 00
     *   Strict => 00, TAG_BUFFER => 00
     */
    private const string DESCRIBE_REQUEST_V1_HEX = '00000025'
        . '0030'
        . '0001'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02'
        . '0a' . '636c69656e742d6964'
        . '00'
        . '07' . '6576656e7473'
        . '00'
        . '00'
        . '00';

    /**
     * A version 1 answer with one entity and one quota of 1024.0 bytes per second.
     */
    private const string DESCRIBE_RESPONSE_V1_HEX = '0000003f'
        . '00000009'
        . '00'
        . '00000000'
        . '0000'
        . '01'
        . '02'
        . '02' . '0a' . '636c69656e742d6964' . '07' . '6576656e7473' . '00'
        . '02' . '13' . '70726f64756365725f627974655f72617465' . '4090000000000000' . '00'
        . '00'
        . '00';

    public function testTheDescribeRequestIsPackedInBothEncodings(): void
    {
        $components = [ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_CLIENT_ID, 'events')->toData()];

        $plain    = new DescribeClientQuotasRequestV0($components, false, 'test', 9);
        $flexible = new DescribeClientQuotasRequest($components, false, 'test', 9);

        self::assertSame(self::DESCRIBE_REQUEST_V0_HEX, bin2hex((string) $plain));
        self::assertSame(self::DESCRIBE_REQUEST_V1_HEX, bin2hex((string) $flexible));
        self::assertSame(ApiKeys::DESCRIBE_CLIENT_QUOTAS, $flexible->getApiKey());
        self::assertFalse(DescribeClientQuotasRequestV0::isFlexible(), 'the version Kafka 2.6 added is plain');
        self::assertTrue(DescribeClientQuotasRequest::isFlexible(), 'and the version Kafka 2.8 added is not');
        self::assertSame(DescribeClientQuotasRequestV0::HEADER_V1, $plain->getHeaderVersion());
        self::assertSame(DescribeClientQuotasRequest::HEADER_V2, $flexible->getHeaderVersion());
    }

    /**
     * The three match types of the filter are the three factory methods of the component
     */
    public function testTheThreeMatchTypesOfAFilterComponent(): void
    {
        $exact     = ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_USER, 'alice')->toData();
        $default   = ClientQuotaFilterComponent::ofDefaultEntity(ClientQuotaEntity::TYPE_USER)->toData();
        $specified = ClientQuotaFilterComponent::ofEntityType(ClientQuotaEntity::TYPE_USER)->toData();

        self::assertSame(ClientQuotaComponentData::MATCH_TYPE_EXACT, $exact->matchType);
        self::assertSame('alice', $exact->match);
        self::assertSame(ClientQuotaComponentData::MATCH_TYPE_DEFAULT, $default->matchType);
        self::assertNull($default->match, 'the default entity has no name to match');
        self::assertSame(ClientQuotaComponentData::MATCH_TYPE_SPECIFIED, $specified->matchType);
        self::assertNull($specified->match);

        self::assertTrue(ClientQuotaFilter::containsOnly([])->strict);
        self::assertFalse(ClientQuotaFilter::contains([])->strict);
        self::assertFalse(ClientQuotaFilter::all()->strict);
        self::assertSame([], ClientQuotaFilter::all()->components, 'and it names no component at all');
    }

    public function testTheDescribeAnswerCarriesTheQuotasAsDoubles(): void
    {
        $response = DescribeClientQuotasResponse::unpack(
            new StringStream((string) hex2bin(self::DESCRIBE_RESPONSE_V1_HEX))
        );

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame('', $response->errorMessage, 'the broker sends the empty string, not null, on success');
        self::assertNotNull($response->entries);
        self::assertCount(1, $response->entries);

        $entry = $response->entries[0];

        self::assertSame(['client-id'], array_keys($entry->entity));
        self::assertSame('events', $entry->entity['client-id']->entityName);
        self::assertSame(1024.0, $entry->values['producer_byte_rate']->value);
        self::assertIsFloat($entry->values['producer_byte_rate']->value);
        self::assertSame(self::DESCRIBE_RESPONSE_V1_HEX, bin2hex((string) $response));
    }

    /**
     * `4090000000000000` is 1024.0 as an IEEE 754 double in network order and nothing else
     */
    public function testTheFloat64TypeIsBigEndian(): void
    {
        $stream = new StringStream('');
        BinarySchema::writeSingleType(BinarySchema::TYPE_FLOAT64, 12.5, $stream);

        self::assertSame('4029000000000000', bin2hex($stream->getBuffer()));
        self::assertSame(8, BinarySchema::getSingleTypeSize(BinarySchema::TYPE_FLOAT64, 12.5));
        self::assertSame(
            12.5,
            BinarySchema::readSingleType(
                BinarySchema::TYPE_FLOAT64,
                new StringStream((string) hex2bin('4029000000000000')),
                'value'
            )
        );
    }

    /**
     * A removal carries a value the broker ignores, because the frame has no optional fields
     */
    public function testARemovalStillCarriesAValue(): void
    {
        $alteration = new ClientQuotaAlteration(
            ClientQuotaEntity::forClientId('events'),
            [ClientQuotaAlterationOp::remove(ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE)]
        );

        $op = $alteration->toData()->ops['producer_byte_rate'];

        self::assertTrue($op->remove);
        self::assertSame(0.0, $op->value, 'the Java client writes NaN here, this one writes zero');

        $request = new AlterClientQuotasRequestV0([$alteration->toData()], false, 'test', 9);

        self::assertStringEndsWith(
            '12' . '70726f64756365725f627974655f72617465' . '0000000000000000' . '01' . '00',
            bin2hex((string) $request),
            'the key, the eight ignored bytes, the remove flag and the validate_only flag'
        );
    }

    /**
     * A `null` entity name is the `<default>` entity of its type, and it is not the name `<default>`
     */
    public function testTheDefaultEntityIsANullName(): void
    {
        $entity = ClientQuotaEntity::defaultOf(ClientQuotaEntity::TYPE_USER);

        self::assertTrue($entity->has(ClientQuotaEntity::TYPE_USER));
        self::assertNull($entity->nameOf(ClientQuotaEntity::TYPE_USER));
        self::assertSame('user=<default>', (string) $entity, 'which is how kafka-configs.sh prints it');
        self::assertNull($entity->toData()[ClientQuotaEntity::TYPE_USER]->entityName, 'but the wire says null');

        $pair = new ClientQuotaEntity([
            ClientQuotaEntity::TYPE_USER      => 'alice',
            ClientQuotaEntity::TYPE_CLIENT_ID => 'events',
        ]);

        self::assertSame('client-id=events,user=alice', (string) $pair, 'a combined entity names both parts');
    }

    /**
     * The answer of the alter api has no top-level error code: one entry per entity, error first
     */
    public function testTheAlterAnswerCarriesOneErrorPerEntity(): void
    {
        $hex = '0000002d'
            . '00000009'
            . '00'
            . '00000000'
            . '02'
            . '002a'
            . '0c' . '4e6f7420616c6c6f776564'
            . '02' . '0a' . '636c69656e742d6964' . '07' . '6576656e7473' . '00'
            . '00'
            . '00';

        $response = AlterClientQuotasResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertCount(1, $response->entries);
        self::assertSame(KafkaException::INVALID_REQUEST, $response->entries[0]->errorCode);
        self::assertSame('Not allowed', $response->entries[0]->errorMessage);
        self::assertSame('events', $response->entries[0]->entity['client-id']->entityName);
        self::assertSame($hex, bin2hex((string) $response));
    }
}
