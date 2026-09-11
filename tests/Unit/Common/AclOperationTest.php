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

namespace Protocol\Kafka\Tests\Unit\Common;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\AclOperation;

/**
 * The bitfield of KIP-430 (Kafka 2.3), which a Metadata v8 and a DescribeGroups v3 answer report.
 *
 * @see docs/protocol/2.8.md, section "The authorized operations (v8, KIP-430)"
 */
#[CoversClass(AclOperation::class)]
final class AclOperationTest extends TestCase
{
    public function testTheCodesAreTheOnesOfTheJavaEnumeration(): void
    {
        // `AclOperation` @ 2.8.2, code by code
        self::assertSame(0, AclOperation::UNKNOWN);
        self::assertSame(1, AclOperation::ANY);
        self::assertSame(2, AclOperation::ALL);
        self::assertSame(3, AclOperation::READ);
        self::assertSame(4, AclOperation::WRITE);
        self::assertSame(5, AclOperation::CREATE);
        self::assertSame(6, AclOperation::DELETE);
        self::assertSame(7, AclOperation::ALTER);
        self::assertSame(8, AclOperation::DESCRIBE);
        self::assertSame(9, AclOperation::CLUSTER_ACTION);
        self::assertSame(10, AclOperation::DESCRIBE_CONFIGS);
        self::assertSame(11, AclOperation::ALTER_CONFIGS);
        self::assertSame(12, AclOperation::IDEMPOTENT_WRITE);
        self::assertSame('IDEMPOTENT_WRITE', AclOperation::NAMES[AclOperation::IDEMPOTENT_WRITE]);
    }

    public function testTheBitfieldsOfTheContainerUnpackToTheOperationsItNamed(): void
    {
        // The two answers of the unsecured container, measured: `metadata.response.v8` carries exactly these
        self::assertSame(
            ['CREATE', 'ALTER', 'DESCRIBE', 'CLUSTER_ACTION', 'DESCRIBE_CONFIGS', 'ALTER_CONFIGS', 'IDEMPOTENT_WRITE'],
            AclOperation::describe(8096),
            'the cluster bitfield of a broker without an authorizer'
        );
        self::assertSame(
            ['READ', 'WRITE', 'CREATE', 'DELETE', 'ALTER', 'DESCRIBE', 'DESCRIBE_CONFIGS', 'ALTER_CONFIGS'],
            AclOperation::describe(3576),
            'and the bitfield of one of its topics'
        );
        self::assertTrue(AclOperation::isAuthorized(3576, AclOperation::READ));
        self::assertFalse(AclOperation::isAuthorized(3576, AclOperation::CLUSTER_ACTION));
    }

    public function testABitfieldRoundTripsThroughTheTwoHelpers(): void
    {
        $operations = [AclOperation::READ, AclOperation::WRITE, AclOperation::DESCRIBE];
        $bitField   = AclOperation::toBitField($operations);

        self::assertSame((1 << 3) | (1 << 4) | (1 << 8), $bitField);
        self::assertSame($operations, AclOperation::fromBitField($bitField));
        self::assertTrue(AclOperation::wasRequested($bitField));
    }

    public function testTheMinimumIntegerIsNotAnEmptySetButAnUnansweredQuestion(): void
    {
        // `Integer.MIN_VALUE` is what a broker writes when the boolean of the request was false; the bitfield 0
        // is a real answer, "this principal may do nothing here"
        self::assertSame(-2147483648, AclOperation::NOT_REQUESTED);
        self::assertFalse(AclOperation::wasRequested(AclOperation::NOT_REQUESTED));
        self::assertSame([], AclOperation::fromBitField(AclOperation::NOT_REQUESTED));
        self::assertFalse(AclOperation::isAuthorized(AclOperation::NOT_REQUESTED, AclOperation::READ));

        self::assertTrue(AclOperation::wasRequested(0));
        self::assertSame([], AclOperation::fromBitField(0));
        self::assertFalse(AclOperation::isAuthorized(0, AclOperation::READ));
    }
}
