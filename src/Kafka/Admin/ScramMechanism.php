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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\Errors\UnsupportedSaslMechanismException;
use Protocol\Kafka\Protocol\Data\ScramCredentialInfo;

/**
 * The two SCRAM mechanisms of Kafka 2.8.2, with what a client needs to derive a salted password for each
 *
 * `ScramMechanism` of the Java admin client, which is an enum of the same two values plus `UNKNOWN`. The `type`
 * is the int8 that travels on the wire; the hash and the key length are RFC 5802 and RFC 7677 (`ScramMechanism`
 * @ 2.8.2: `SCRAM-SHA-256` is `SHA-256`/32 bytes, `SCRAM-SHA-512` is `SHA-512`/64).
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
enum ScramMechanism: int
{
    case ScramSha256 = ScramCredentialInfo::MECHANISM_SCRAM_SHA_256;
    case ScramSha512 = ScramCredentialInfo::MECHANISM_SCRAM_SHA_512;

    /**
     * The smallest iteration count both mechanisms of Kafka 2.8.2 accept (`ScramMechanism.minIterations()`)
     */
    public const int MIN_ITERATIONS = 4096;

    /**
     * The name the SASL handshake and `kafka-configs.sh` use, e.g. `SCRAM-SHA-256`
     */
    public function mechanismName(): string
    {
        return match ($this) {
            self::ScramSha256 => 'SCRAM-SHA-256',
            self::ScramSha512 => 'SCRAM-SHA-512',
        };
    }

    /**
     * The hash algorithm of this mechanism, as PHP's hash extension names it
     */
    public function hashAlgorithm(): string
    {
        return match ($this) {
            self::ScramSha256 => 'sha256',
            self::ScramSha512 => 'sha512',
        };
    }

    /**
     * Length of the salted password of this mechanism, in bytes
     */
    public function keyLength(): int
    {
        return match ($this) {
            self::ScramSha256 => 32,
            self::ScramSha512 => 64,
        };
    }

    /**
     * Derives the salted password the api expects: `Hi(password, salt, iterations)` of RFC 5802
     *
     * This is the one computation a client of KIP-554 has to do itself, and it is why the broker never sees a
     * password. `ScramFormatter.hi()` @ 2.8.2 is `PBKDF2WithHmac<hash>` with the same three inputs.
     */
    public function saltedPassword(string $password, string $salt, int $iterations): string
    {
        return hash_pbkdf2($this->hashAlgorithm(), $password, $salt, $iterations, $this->keyLength(), true);
    }

    /**
     * Returns the mechanism of an int8 of the wire
     *
     * @throws UnsupportedSaslMechanismException If the broker named a mechanism this client does not know
     */
    public static function fromType(int $type): self
    {
        return self::tryFrom($type) ?? throw new UnsupportedSaslMechanismException(
            ['mechanism' => $type, 'error' => 'The broker named a SCRAM mechanism this client does not know']
        );
    }
}
