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

/**
 * One change {@see AdminClient::alterUserScramCredentials()} applies: a {@see UserScramCredentialUpsertion} or a
 * {@see UserScramCredentialDeletion}
 *
 * `UserScramCredentialAlteration` of the Java admin client, which is the abstract base of the same two classes.
 * The request carries the two kinds in **two arrays**, so this interface exists to let a caller hand over one
 * ordered list and let the client sort them.
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
interface UserScramCredentialAlteration
{
    /**
     * Returns the name of the user this alteration is about
     */
    public function user(): string;
}
