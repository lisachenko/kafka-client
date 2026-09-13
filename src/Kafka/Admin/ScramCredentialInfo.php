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
 * One SCRAM credential of a user, as {@see AdminClient::describeUserScramCredentials()} reports it
 *
 * `ScramCredentialInfo` of the Java admin client: the mechanism and the iteration count, and nothing else - the
 * salted password is what makes a credential a secret, and no api of Kafka ever sends it back.
 *
 * @see docs/protocol/2.8.md, section "DescribeUserScramCredentials API (key 50, v0)"
 */
final class ScramCredentialInfo
{
    public function __construct(
        public readonly ScramMechanism $mechanism,
        public readonly int $iterations
    ) {}
}
