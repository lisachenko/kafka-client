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

namespace Protocol\Kafka\Tests\Unit\Common\Security;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Tests the principal struct that the four delegation token APIs of KIP-48 embed.
 *
 * @see docs/protocol/2.8.md, section "CreateDelegationToken API (key 38, v0)"
 */
#[CoversClass(KafkaPrincipal::class)]
final class KafkaPrincipalTest extends TestCase
{
    /**
     * `User:kafkatest` on the wire: two length-prefixed strings and nothing else
     */
    private const string PRINCIPAL_HEX = '0004' . '55736572' . '0009' . '6b61666b6174657374';

    public function testThePrincipalIsPackedAsTwoStrings(): void
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream(KafkaPrincipal::user('kafkatest'), $stream);

        self::assertSame(self::PRINCIPAL_HEX, bin2hex($stream->getBuffer()));
    }

    public function testThePrincipalIsUnpackedAsTwoStrings(): void
    {
        $principal = BinarySchema::readObjectFromStream(
            KafkaPrincipal::class,
            new StringStream((string) hex2bin(self::PRINCIPAL_HEX))
        );

        self::assertInstanceOf(KafkaPrincipal::class, $principal);
        self::assertSame('User', $principal->principalType);
        self::assertSame('kafkatest', $principal->name);
    }

    public function testTheUserTypeIsTheOneOfTheShippedAuthorizer(): void
    {
        // `KafkaPrincipal.USER_TYPE` @ 1.1.1, the only type the delegation token apis accept in a renewer
        self::assertSame('User', KafkaPrincipal::USER_TYPE);
        self::assertSame('User', KafkaPrincipal::user('anybody')->principalType);
    }

    public function testAnUnauthenticatedChannelIsTheAnonymousPrincipal(): void
    {
        // `KafkaPrincipal.ANONYMOUS` @ 1.1.1, the owner a PLAINTEXT listener answers a create request with
        self::assertSame('User:ANONYMOUS', (string) KafkaPrincipal::anonymous());
    }

    public function testThePrincipalIsPrintedAsTheKafkaToolsPrintIt(): void
    {
        self::assertSame('User:kafkatest', (string) KafkaPrincipal::user('kafkatest'));
        self::assertSame('Group:analytics', (string) new KafkaPrincipal('Group', 'analytics'));
    }

    /**
     * @return iterable<string, array{0: string, 1: string, 2: string}>
     */
    public static function principalStrings(): iterable
    {
        yield 'an ordinary user'      => ['User:kafkatest', 'User', 'kafkatest'];
        yield 'another type'          => ['Group:analytics', 'Group', 'analytics'];
        // `SecurityUtils.parseKafkaPrincipal` splits on the FIRST colon, so a name may contain colons of its own
        yield 'a name with a colon'   => ['User:CN=kafka:9092', 'User', 'CN=kafka:9092'];
    }

    #[DataProvider('principalStrings')]
    public function testAPrincipalIsParsedFromItsStringForm(string $input, string $type, string $name): void
    {
        $principal = KafkaPrincipal::fromString($input);

        self::assertSame($type, $principal->principalType);
        self::assertSame($name, $principal->name);
        self::assertSame($input, (string) $principal);
    }

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function malformedPrincipalStrings(): iterable
    {
        yield 'no separator at all' => ['kafkatest'];
        yield 'no type'             => [':kafkatest'];
        yield 'no name'             => ['User:'];
        yield 'nothing'             => [''];
    }

    #[DataProvider('malformedPrincipalStrings')]
    public function testAStringThatIsNotAPrincipalIsRefused(string $input): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('<principalType>:<principalName>');

        KafkaPrincipal::fromString($input);
    }

    public function testTwoPrincipalsAreEqualWhenBothTheirPartsAre(): void
    {
        $principal = KafkaPrincipal::user('kafkatest');

        self::assertTrue($principal->equals(KafkaPrincipal::user('kafkatest')));
        self::assertFalse($principal->equals(KafkaPrincipal::user('admin')));
        self::assertFalse($principal->equals(new KafkaPrincipal('Group', 'kafkatest')));
    }

    public function testAListOfPrincipalsAcceptsObjectsAndStringsAlike(): void
    {
        $principals = KafkaPrincipal::listOf(['User:admin', KafkaPrincipal::user('kafkatest')]);

        self::assertSame(['User:admin', 'User:kafkatest'], array_map(strval(...), $principals));
        self::assertSame([], KafkaPrincipal::listOf([]));
    }
}
