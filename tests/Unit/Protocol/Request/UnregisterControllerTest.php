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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\UnregisterControllerRequest;
use Protocol\Kafka\Protocol\Request\UnregisterControllerResponse;

/**
 * UnregisterController v0 (key 94, Kafka 4.4, KIP-1312): one int32 in, the code and the message out
 *
 * The frames are the ones the 4.4.0 node exchanged for the unknown controller id 4242 (`unregistercontroller.*.v0.unknown`).
 *
 * @see docs/protocol/overview.md, section "UnregisterController API (key 94, v0)"
 */
#[CoversClass(UnregisterControllerRequest::class)]
#[CoversClass(UnregisterControllerResponse::class)]
final class UnregisterControllerTest extends TestCase
{
    /**
     * The request for the controller id 4242, flexible from its version 0.
     *
     *   header v2: apiKey 005e (94), apiVersion 0000, correlationId 0000113a (4410),
     *              clientId 0012 "kafka-client-t4-44" (never compact), TAG_BUFFER 00
     *   ControllerId => 00001092 (4242), TAG_BUFFER => 00
     */
    private const string REQUEST_HEX = '00000022'
        . '005e' . '0000' . '0000113a' . '0012' . '6b61666b612d636c69656e742d74342d3434' . '00'
        . '00001092' . '00';

    private const string MESSAGE = 'Controller ID 4242 is not currently registered.';

    public function testTheRequestIsTheControllerIdAlone(): void
    {
        $request = new UnregisterControllerRequest(4242, 'kafka-client-t4-44', 4410);

        self::assertSame(ApiKeys::UNREGISTER_CONTROLLER, $request->getApiKey());
        self::assertSame(94, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertTrue(UnregisterControllerRequest::isFlexible(), 'the api is flexible from its first version');
        self::assertSame(4242, $request->getControllerId());
        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(
            self::REQUEST_HEX,
            bin2hex((string) UnregisterControllerRequest::unpack(new StringStream((string) $request)))
        );
    }

    public function testTheAnswerIsTheCodeAndTheMessageOfTheController(): void
    {
        $hex = '0000003c' . '0000113a' . '00' . '00000000' . '0088' . '30' . bin2hex(self::MESSAGE) . '00';

        $response = UnregisterControllerResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::CONTROLLER_ID_NOT_REGISTERED, $response->errorCode);
        self::assertSame(136, $response->errorCode);
        self::assertSame(self::MESSAGE, $response->errorMessage, 'a compact string of 47 bytes, its length 47 + 1');
        self::assertSame($hex, bin2hex((string) $response));
    }

    public function testASuccessfulAnswerCarriesTheNullMessage(): void
    {
        $hex = '0000000d' . '00000007' . '00' . '00000000' . '0000' . '00' . '00';

        $response = UnregisterControllerResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertNull($response->errorMessage, 'the compact null is the one byte 00');
        self::assertSame($hex, bin2hex((string) $response));
    }
}
