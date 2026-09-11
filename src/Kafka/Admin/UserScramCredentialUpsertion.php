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

use InvalidArgumentException;
use Protocol\Kafka\Protocol\Data\ScramCredentialUpsertion;

/**
 * A credential {@see AdminClient::alterUserScramCredentials()} writes for a user
 *
 * `UserScramCredentialUpsertion` of the Java admin client, and like it this class **derives the salted password
 * itself**: the constructor takes the password, the api takes `Hi(password, salt, iterations)`, and the broker
 * never sees the first of the two. A salt is generated when none is given.
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
final class UserScramCredentialUpsertion implements UserScramCredentialAlteration
{
    public readonly string $salt;

    /**
     * @param string         $user       Name of the user
     * @param ScramMechanism $mechanism  Mechanism of the credential
     * @param string         $password   Password to derive the credential from; it never leaves this process
     * @param int            $iterations Number of PBKDF2 iterations, at least {@see ScramMechanism::MIN_ITERATIONS}
     * @param string|null    $salt       Salt to use, null for 24 fresh random bytes
     */
    public function __construct(
        public readonly string $user,
        public readonly ScramMechanism $mechanism,
        private readonly string $password,
        public readonly int $iterations = ScramMechanism::MIN_ITERATIONS,
        ?string $salt = null
    ) {
        if ($user === '') {
            throw new InvalidArgumentException('The user of a SCRAM credential can not be empty');
        }
        $this->salt = $salt ?? random_bytes(24);
    }

    /**
     * Returns the name of the user this alteration is about
     */
    public function user(): string
    {
        return $this->user;
    }

    /**
     * Returns this upsertion in the shape the api puts on the wire, deriving the salted password on the way
     */
    public function toData(): ScramCredentialUpsertion
    {
        return new ScramCredentialUpsertion(
            $this->user,
            $this->mechanism->value,
            $this->iterations,
            $this->salt,
            $this->mechanism->saltedPassword($this->password, $this->salt, $this->iterations)
        );
    }
}
