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

use Protocol\Kafka\Protocol\Data\ScramCredentialDeletion;

/**
 * A credential {@see AdminClient::alterUserScramCredentials()} removes from a user
 *
 * `UserScramCredentialDeletion` of the Java admin client. A user may hold one credential per mechanism, so a
 * deletion names both.
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
final class UserScramCredentialDeletion implements UserScramCredentialAlteration
{
    public function __construct(public readonly string $user, public readonly ScramMechanism $mechanism) {}

    /**
     * Returns the name of the user this alteration is about
     */
    public function user(): string
    {
        return $this->user;
    }

    /**
     * Returns this deletion in the shape the api puts on the wire
     */
    public function toData(): ScramCredentialDeletion
    {
        return new ScramCredentialDeletion($this->user, $this->mechanism->value);
    }
}
