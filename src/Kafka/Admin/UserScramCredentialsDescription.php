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
 * Everything {@see AdminClient::describeUserScramCredentials()} knows about one user
 *
 * `UserScramCredentialsDescription` of the Java admin client.
 *
 * @see docs/protocol/2.8.md, section "DescribeUserScramCredentials API (key 50, v0)"
 */
final class UserScramCredentialsDescription
{
    /**
     * @param string                                  $name            Name of the user
     * @param array<int, ScramCredentialInfo>         $credentialInfos Credentials of the user, by mechanism value
     */
    public function __construct(public readonly string $name, public readonly array $credentialInfos) {}

    /**
     * Returns whether the user has a credential for a mechanism
     */
    public function has(ScramMechanism $mechanism): bool
    {
        return isset($this->credentialInfos[$mechanism->value]);
    }

    /**
     * Returns the credential of one mechanism, or null when the user has none for it
     */
    public function credentialFor(ScramMechanism $mechanism): ?ScramCredentialInfo
    {
        return $this->credentialInfos[$mechanism->value] ?? null;
    }
}
