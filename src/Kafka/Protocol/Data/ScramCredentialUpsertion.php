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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One credential an AlterUserScramCredentials request writes (key 51, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   ScramCredentialUpsertion => Name Mechanism Iterations Salt SaltedPassword
 *     Name           => COMPACT_STRING
 *     Mechanism      => INT8
 *     Iterations     => INT32
 *     Salt           => COMPACT_BYTES
 *     SaltedPassword => COMPACT_BYTES
 * </pre>
 *
 * **The client derives the salted password, the broker never sees the password.** `SaltedPassword` is
 * `Hi(password, salt, iterations)` of RFC 5802, i.e. PBKDF2-HMAC with the hash of the mechanism - in PHP
 * `hash_pbkdf2('sha256', $password, $salt, $iterations, 32, true)` for SCRAM-SHA-256 and `'sha512'` with a length
 * of 64 for SCRAM-SHA-512. `ScramFormatter.hi()` @ 2.8.2 is the same function on the Java side, and
 * `kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=…]'` does exactly this before it sends the
 * request.
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
class ScramCredentialUpsertion implements BinarySchemaInterface
{
    /**
     * Name of the user whose credential is written
     */
    public string $name;

    /**
     * Mechanism of the credential, one of the MECHANISM_* constants of {@see ScramCredentialInfo}
     */
    public int $mechanism;

    /**
     * Number of PBKDF2 iterations, at least 4096 for both mechanisms of Kafka 2.8.2
     */
    public int $iterations;

    /**
     * Random salt the client generated
     */
    public string $salt;

    /**
     * The salted password, i.e. `Hi(password, salt, iterations)` of RFC 5802
     */
    public string $saltedPassword;

    public function __construct(
        string $name = '',
        int $mechanism = 0,
        int $iterations = 0,
        string $salt = '',
        string $saltedPassword = ''
    ) {
        $this->name           = $name;
        $this->mechanism      = $mechanism;
        $this->iterations     = $iterations;
        $this->salt           = $salt;
        $this->saltedPassword = $saltedPassword;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'           => BinarySchema::TYPE_STRING,
            'mechanism'      => BinarySchema::TYPE_INT8,
            'iterations'     => BinarySchema::TYPE_INT32,
            'salt'           => BinarySchema::TYPE_BYTEARRAY,
            'saltedPassword' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
