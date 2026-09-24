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
use Protocol\Kafka\Common\AccessControlEntry;
use Protocol\Kafka\Common\AccessControlEntryFilter;
use Protocol\Kafka\Common\AclBinding;
use Protocol\Kafka\Common\AclBindingFilter;
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\AclPermissionType;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\PatternType;
use Protocol\Kafka\Common\ResourcePattern;
use Protocol\Kafka\Common\ResourcePatternFilter;
use Protocol\Kafka\Common\ResourceType;
use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\CreateAclsResponseResult;
use Protocol\Kafka\Protocol\Data\DeleteAclsResponseFilterResult;
use Protocol\Kafka\Protocol\Data\DeleteAclsResponseMatchingAcl;
use Protocol\Kafka\Protocol\Data\DescribeAclsResponseResource;
use Protocol\Kafka\Protocol\Request\CreateAclsRequest;
use Protocol\Kafka\Protocol\Request\CreateAclsResponse;
use Protocol\Kafka\Protocol\Request\DeleteAclsRequest;
use Protocol\Kafka\Protocol\Request\DeleteAclsResponse;
use Protocol\Kafka\Protocol\Request\DescribeAclsRequest;
use Protocol\Kafka\Protocol\Request\DescribeAclsResponse;
use Protocol\Kafka\Tests\Compliance\VectorFile;

/**
 * Byte-exact tests for the three ACL apis (keys 29, 30 and 31) at the version 3 of Kafka 3.3 (KIP-140, KIP-373).
 *
 * The three apis are implemented on this line for the first time and at the version 3 alone, so every frame below
 * is one the 3.9.2 KRaft node with its `StandardAuthorizer` really sent or really accepted.
 *
 * @see docs/protocol/4.3.md, sections "DescribeAcls API (key 29, v0 to v3)", "CreateAcls API (key 30, v0 to v3)"
 *      and "DeleteAcls API (key 31, v0 to v3)"
 */
#[CoversClass(DescribeAclsRequest::class)]
#[CoversClass(DescribeAclsResponse::class)]
#[CoversClass(CreateAclsRequest::class)]
#[CoversClass(CreateAclsResponse::class)]
#[CoversClass(DeleteAclsRequest::class)]
#[CoversClass(DeleteAclsResponse::class)]
#[CoversClass(DescribeAclsResponseResource::class)]
#[CoversClass(CreateAclsResponseResult::class)]
#[CoversClass(DeleteAclsResponseFilterResult::class)]
#[CoversClass(DeleteAclsResponseMatchingAcl::class)]
#[CoversClass(AclBinding::class)]
#[CoversClass(AclBindingFilter::class)]
#[CoversClass(AccessControlEntry::class)]
#[CoversClass(AccessControlEntryFilter::class)]
#[CoversClass(ResourcePattern::class)]
#[CoversClass(ResourcePatternFilter::class)]
#[CoversClass(ResourceType::class)]
#[CoversClass(PatternType::class)]
#[CoversClass(AclPermissionType::class)]
final class AclApisTest extends TestCase
{
    private const string TOPIC = 't4-33-acl-topic';

    private const string GROUP_PREFIX = 't4-33-acl-';

    private const string PRINCIPAL = 'User:acltest';

    public function testTheDescribeRequestIsTheSevenFlatFieldsOfAFilter(): void
    {
        $request = new DescribeAclsRequest(null, 't4-33-vectors', 3300);

        // The default filter matches everything: ANY for the three codes and a compact null for the three names
        self::assertSame(self::vector('describe-acls', 'describeacls.request.v3.no-acl'), bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_ACLS, $request->getApiKey());
        self::assertSame(3, $request->getApiVersion(), 'Kafka 3.3 added the version 3 and this client sends it');
        self::assertTrue(DescribeAclsRequest::isFlexible(), 'Kafka 2.4 made the version 2 the first flexible one');
        self::assertStringEndsWith('01000100000101' . '00', bin2hex((string) $request));
    }

    public function testAFilterOfTheRequestCanBeReadBackFromIt(): void
    {
        $filter  = AclBindingFilter::matching(ResourceType::GROUP, self::GROUP_PREFIX . 'group-one');
        $request = new DescribeAclsRequest($filter, 't4-33-vectors', 3305);

        self::assertSame(
            self::vector('describe-acls', 'describeacls.request.v3.match'),
            bin2hex((string) $request),
            'the pattern type MATCH (2) is a filter value that no stored acl ever carries'
        );
        self::assertSame(ResourceType::GROUP, $request->getFilter()->patternFilter->resourceType);
        self::assertSame(PatternType::MATCH, $request->getFilter()->patternFilter->patternType);
        self::assertNull($request->getFilter()->entryFilter->principal, 'everything else stayed a wildcard');
    }

    public function testTheDescribeAnswerIsGroupedByResourcePattern(): void
    {
        $answer = DescribeAclsResponse::unpack(new StringStream(
            (string) hex2bin(self::vector('describe-acls', 'describeacls.response.v3'))
        ));

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame(
            '',
            $answer->errorMessage,
            'the field of a successful answer is the EMPTY string, not null: the generated data class starts with ""'
        );
        self::assertCount(2, $answer->resources, 'one entry per pattern, whatever the acls inside it are');

        $bindings = $answer->bindings();
        self::assertCount(2, $bindings);
        self::assertSame(
            'GROUP:PREFIXED:' . self::GROUP_PREFIX . ' ' . self::PRINCIPAL . ' * READ ALLOW',
            (string) $bindings[0]
        );
        self::assertSame('TOPIC:LITERAL:' . self::TOPIC . ' ' . self::PRINCIPAL . ' * READ ALLOW', (string) $bindings[1]);
        self::assertSame(AclOperation::READ, $bindings[0]->entry->operation);
        self::assertSame(AclPermissionType::ALLOW, $bindings[0]->entry->permissionType);
        self::assertSame(AccessControlEntry::ANY_HOST, $bindings[0]->entry->host, 'an acl without a host is `*`');
    }

    public function testTheRefusedDescribeCarriesTheWholeRequestObjectOfTheBroker(): void
    {
        $answer = DescribeAclsResponse::unpack(new StringStream(
            (string) hex2bin(self::vector('describe-acls', 'describeacls.response.v3.unauthorized'))
        ));

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $answer->errorCode);
        self::assertSame([], $answer->resources, 'the filter is never applied at all');
        self::assertStringStartsWith('Request Request(processor=', (string) $answer->errorMessage);
        self::assertStringContainsString('ListenerName(SASL_PLAINTEXT)', (string) $answer->errorMessage);
    }

    public function testTheCreateRequestCarriesTheSevenFieldsOfEveryAclFlat(): void
    {
        $request = new CreateAclsRequest(
            [
                AclBinding::allow(ResourceType::TOPIC, self::TOPIC, self::PRINCIPAL, AclOperation::READ),
                new AclBinding(
                    ResourcePattern::prefixed(ResourceType::GROUP, self::GROUP_PREFIX),
                    AccessControlEntry::allow(self::PRINCIPAL, AclOperation::READ)
                ),
            ],
            't4-33-vectors',
            3301
        );

        self::assertSame(self::vector('create-acls', 'createacls.request.v3'), bin2hex((string) $request));
        self::assertSame(ApiKeys::CREATE_ACLS, $request->getApiKey());
        self::assertSame(3, $request->getApiVersion());
        self::assertCount(2, $request->getCreations());
    }

    public function testTheCreateAnswerIsOneResultPerCreationAndHasNoTopLevelCode(): void
    {
        $answer = CreateAclsResponse::unpack(new StringStream(
            (string) hex2bin(self::vector('create-acls', 'createacls.response.v3'))
        ));

        self::assertCount(2, $answer->results, 'one result per creation, in the order of the request');
        foreach ($answer->results as $result) {
            self::assertSame(KafkaException::NO_ERROR, $result->errorCode);
            self::assertSame('', $result->errorMessage, 'a creation that worked carries the empty string');
        }
        self::assertArrayNotHasKey(
            'errorCode',
            array_diff_key(CreateAclsResponse::getScheme(), ['throttleTimeMs' => null, 'results' => null]),
            'the api has no top-level error code at all'
        );
    }

    public function testARefusedCreationIsReportedInItsOwnEntry(): void
    {
        $answer = CreateAclsResponse::unpack(new StringStream(
            (string) hex2bin(self::vector('create-acls', 'createacls.response.v3.invalid'))
        ));

        self::assertCount(1, $answer->results);
        self::assertSame(KafkaException::INVALID_REQUEST, $answer->results[0]->errorCode);
        self::assertSame('Invalid empty resource name', $answer->results[0]->errorMessage);
    }

    public function testAPatternTypeAFilterOnlyKnowsIsAnUnknownServerError(): void
    {
        // `CreateAclsRequest.aclBinding` @ 3.9.2 builds a ResourcePattern before any validation of the api runs,
        // and its constructor throws the plain IllegalArgumentException("patternType must not be ANY")
        $answer = CreateAclsResponse::unpack(new StringStream(
            (string) hex2bin(self::vector('create-acls', 'createacls.response.v3.pattern-type-any'))
        ));

        self::assertSame(KafkaException::UNKNOWN, $answer->results[0]->errorCode);
        self::assertNull($answer->results[0]->errorMessage, 'and not even a message comes with it');
        self::assertFalse(PatternType::isSpecific(PatternType::ANY), 'which is what a client checks beforehand');
        self::assertTrue(PatternType::isSpecific(PatternType::LITERAL));
        self::assertTrue(PatternType::isSpecific(PatternType::PREFIXED));
    }

    public function testTheDeleteRequestIsAnArrayOfTheSameFiltersTheDescribeApiTakes(): void
    {
        $request = new DeleteAclsRequest(
            [
                AclBindingFilter::ofResourceType(ResourceType::TOPIC),
                AclBindingFilter::ofResourceType(ResourceType::TRANSACTIONAL_ID),
            ],
            't4-33-vectors',
            3313
        );

        self::assertSame(self::vector('delete-acls', 'deleteacls.request.v3'), bin2hex((string) $request));
        self::assertSame(ApiKeys::DELETE_ACLS, $request->getApiKey());
        self::assertCount(2, $request->getFilters());
    }

    public function testTheDeleteAnswerRepeatsEveryMatchedAclInFull(): void
    {
        $answer = DeleteAclsResponse::unpack(new StringStream(
            (string) hex2bin(self::vector('delete-acls', 'deleteacls.response.v3'))
        ));

        self::assertCount(2, $answer->filterResults, 'one result per filter of the request, in its order');

        $matched = $answer->filterResults[0]->matchingAcls;
        self::assertCount(1, $matched);
        self::assertSame(KafkaException::NO_ERROR, $matched[0]->errorCode);
        self::assertSame(
            'TOPIC:LITERAL:' . self::TOPIC . ' ' . self::PRINCIPAL . ' * READ ALLOW',
            (string) $matched[0]->binding,
            'a delete names a filter, so the answer is the only place that says what was removed'
        );

        self::assertSame(KafkaException::NO_ERROR, $answer->filterResults[1]->errorCode);
        self::assertSame([], $answer->filterResults[1]->matchingAcls, 'a filter that matched nothing is not an error');
        self::assertNull(
            $answer->filterResults[0]->errorMessage,
            'a successful filter carries a NULL message, where a successful creation carries the empty string'
        );
        self::assertSame([(string) $matched[0]->binding], array_map(strval(...), $answer->deletedBindings()));
    }

    public function testAFilterOfOneAclNamesEveryFieldOfIt(): void
    {
        $binding = AclBinding::allow(ResourceType::TOPIC, self::TOPIC, self::PRINCIPAL, AclOperation::READ);
        $filter  = AclBindingFilter::of($binding);

        self::assertTrue($filter->matchesAtMostOne(), 'every one of the seven fields is concrete');
        self::assertFalse(AclBindingFilter::any()->matchesAtMostOne());
        self::assertFalse(AclBindingFilter::ofResourceType(ResourceType::TOPIC)->matchesAtMostOne());
        self::assertSame(
            self::vector('delete-acls', 'deleteacls.request.v3.unauthorized'),
            bin2hex((string) new DeleteAclsRequest([$filter], 't4-33-vectors', 3312))
        );
    }

    public function testTheUserResourceAndItsTwoOperationsAreTheOnesKafkaThreeThreeAdded(): void
    {
        $acl = AclBinding::allow(ResourceType::USER, 'acltest', self::PRINCIPAL, AclOperation::CREATE_TOKENS);

        self::assertSame(7, ResourceType::USER, 'ResourceType.USER @ 3.9.2');
        self::assertSame(13, AclOperation::CREATE_TOKENS, 'AclOperation.CREATE_TOKENS @ 3.9.2 (KIP-373)');
        self::assertSame(14, AclOperation::DESCRIBE_TOKENS);
        self::assertSame('USER', ResourceType::nameOf(ResourceType::USER));
        self::assertSame('CREATE_TOKENS', AclOperation::nameOf(AclOperation::CREATE_TOKENS));
        self::assertSame(
            self::vector('create-acls', 'createacls.request.v3.user-resource'),
            bin2hex((string) new CreateAclsRequest([$acl], 't4-33-vectors', 3306))
        );
    }

    public function testAPrincipalObjectAndItsStringFormAreTheSameFilter(): void
    {
        $fromObject = AclBindingFilter::ofPrincipal(KafkaPrincipal::user('acltest'));
        $fromString = AclBindingFilter::ofPrincipal(self::PRINCIPAL);

        self::assertSame(self::PRINCIPAL, $fromObject->entryFilter->principal);
        self::assertSame(
            bin2hex((string) new DescribeAclsRequest($fromString, 't4-33-vectors', 3304)),
            bin2hex((string) new DescribeAclsRequest($fromObject, 't4-33-vectors', 3304))
        );
        self::assertSame(
            self::vector('describe-acls', 'describeacls.request.v3.by-principal'),
            bin2hex((string) new DescribeAclsRequest($fromObject, 't4-33-vectors', 3304))
        );
    }

    public function testTheCodesOfTheFourEnumerationsAreTheOnesOfTheJavaClient(): void
    {
        self::assertSame([0, 1, 2, 3, 4, 5, 6, 7], array_keys(ResourceType::NAMES));
        self::assertSame([0, 1, 2, 3, 4], array_keys(PatternType::NAMES));
        self::assertSame([0, 1, 2, 3], array_keys(AclPermissionType::NAMES));
        self::assertSame('kafka-cluster', ResourceType::CLUSTER_NAME, 'Resource.CLUSTER_NAME');
        self::assertSame('*', PatternType::WILDCARD_NAME, 'ResourcePattern.WILDCARD_RESOURCE');
        self::assertSame(
            ResourceType::CLUSTER_NAME,
            ResourcePattern::cluster()->resourceName,
            'the one cluster resource is always literal and always carries that name'
        );
        self::assertSame('ALLOW', AclPermissionType::nameOf(AclPermissionType::ALLOW));
        self::assertSame('99', AclPermissionType::nameOf(99), 'a code this client does not know prints itself');
    }

    /**
     * Returns the raw hex of a documented wire vector
     */
    private static function vector(string $api, string $id): string
    {
        foreach (VectorFile::read($api)['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) $vector['hex'];
            }
        }

        self::fail("There is no wire vector {$id} in docs/protocol/vectors/{$api}.json");
    }
}
