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
 * One SCRAM credential of a user, as DescribeUserScramCredentials describes it (key 50, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   CredentialInfo => Mechanism Iterations
 *     Mechanism  => INT8   (1 SCRAM-SHA-256, 2 SCRAM-SHA-512)
 *     Iterations => INT32
 * </pre>
 *
 * **The api never answers the credential itself**, and there is no field for it: a SCRAM credential is a salted
 * password, and knowing it is knowing the password. What can be described is which mechanisms a user has a
 * credential for and how many PBKDF2 iterations each of them uses - `ScramCredentialInfo` of the Java admin client.
 *
 * @see docs/protocol/2.8.md, section "DescribeUserScramCredentials API (key 50, v0)"
 */
class ScramCredentialInfo implements BinarySchemaInterface
{
    /**
     * `ScramMechanism.SCRAM_SHA_256` @ 2.8.2, the mechanism this repository exercises
     */
    public const int MECHANISM_SCRAM_SHA_256 = 1;

    /**
     * `ScramMechanism.SCRAM_SHA_512` @ 2.8.2
     */
    public const int MECHANISM_SCRAM_SHA_512 = 2;

    /**
     * Mechanism of this credential, one of the MECHANISM_* constants
     */
    public int $mechanism;

    /**
     * Number of PBKDF2 iterations the salted password was derived with
     */
    public int $iterations;

    public function __construct(int $mechanism = 0, int $iterations = 0)
    {
        $this->mechanism  = $mechanism;
        $this->iterations = $iterations;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'mechanism'  => BinarySchema::TYPE_INT8,
            'iterations' => BinarySchema::TYPE_INT32,
        ];
    }
}
