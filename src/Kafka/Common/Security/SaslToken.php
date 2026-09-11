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

namespace Protocol\Kafka\Common\Security;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One SASL token as it travels over a `SASL_PLAINTEXT`/`SASL_SSL` connection.
 *
 * Between the SaslHandshake response and the first ordinary request, the connection carries the tokens of the
 * chosen mechanism and nothing else. After a **v0** handshake they are **not** Kafka requests: they have no api
 * key, no api version, no correlation id and no client id - a token is a size-prefixed blob, exactly what the
 * `bytes` primitive of the protocol is:
 *
 * <pre>
 *   SaslToken => Token
 *     Token => bytes    // INT32 length, then that many bytes
 * </pre>
 *
 * `SaslServerAuthenticator.authenticate()` @ 1.1.1 reads such a frame with a plain `NetworkReceive` (the same
 * 4-byte length prefix every Kafka frame carries) and hands the payload straight to
 * `SaslServer.evaluateResponse()`; the answer travels back the same way, and for PLAIN it is the empty token
 * `new byte[0]`, i.e. the four bytes `00 00 00 00`.
 *
 * Kafka 1.0 (KIP-152) wrapped the very same payload into the `SaslAuthenticate` request (api key 36) to give it a
 * header and an error code, which is what a **v1** handshake asks for: this class then only builds the bytes and
 * {@see \Protocol\Kafka\Protocol\Request\SaslAuthenticateRequest} carries them. The payload is identical in both
 * exchanges - what changes is the framing around it.
 *
 * For the PLAIN mechanism the token is the RFC 4616 message `authzid \0 authcid \0 passwd`, which
 * `PlainSaslServer.evaluateResponse()` splits on the NUL bytes into exactly three parts; the authorization id is
 * left empty by this client, as the Java `PlainLoginModule` does.
 *
 * @see docs/protocol/2.8.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 * @see \Protocol\Kafka\IO\SocketStream::authenticate()
 */
final class SaslToken implements BinarySchemaInterface
{
    /**
     * Payload of the token, `null` for the length -1 that the `bytes` primitive defines as null
     */
    public ?string $token;

    /**
     * @param string|null $token Payload of the token, an empty string for the empty answer of the broker
     */
    public function __construct(?string $token = '')
    {
        $this->token = $token;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'token' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Builds the initial response of the PLAIN mechanism, `authzid \0 authcid \0 passwd` (RFC 4616)
     *
     * @param string $username        Name of the user, the `authcid` - `user_<name>` in the JAAS file of the broker
     * @param string $password        Password of that user, sent in clear text inside the token
     * @param string $authorizationId Identity to act as, empty to act as the authenticated user itself
     */
    public static function ofPlainCredentials(
        string $username,
        string $password,
        string $authorizationId = ''
    ): self {
        return new self($authorizationId . "\0" . $username . "\0" . $password);
    }

    /**
     * Reads the next token frame from a connection
     */
    public static function readFrom(Stream $stream): self
    {
        /** @var self $token */
        $token = BinarySchema::readObjectFromStream(self::class, $stream);

        return $token;
    }

    /**
     * Writes this token to a connection, length prefix included
     */
    public function writeTo(Stream $stream): void
    {
        BinarySchema::writeObjectToStream($this, $stream);
    }

    /**
     * Returns the frame of this token - the length prefix and the payload - as a string
     */
    public function pack(): string
    {
        $stream = new StringStream();
        $this->writeTo($stream);

        return $stream->getBuffer();
    }

    /**
     * Restores a token from its frame, i.e. from the length prefix and the payload
     *
     * @param string $bytes Complete token frame, as it travels over the connection
     */
    public static function unpack(string $bytes): self
    {
        return self::readFrom(new StringStream($bytes));
    }

    /**
     * Tells whether this is the empty token that the broker answers a successful PLAIN authentication with
     */
    public function isEmpty(): bool
    {
        return $this->token === null || $this->token === '';
    }
}
