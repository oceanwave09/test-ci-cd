# Changelog

<!--next-version-placeholder-->

## v0.6.1 (2023-07-14)

### Fix

* Claim, coverage transformer templates ([`7f3fc65`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/7f3fc65692e1cdae6628d168380255af6ed079e9))

## v0.6.0 (2023-07-04)

### Feature

* Added CCDA, HL7 omniparser config initial version ([`d4e5a66`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/d4e5a66de1c5ac9f8c36e461c289e6fc50c09920))

## v0.5.0 (2023-06-19)

### Feature

* Added support to write metadata on canonical file ([`3febd6e`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/3febd6e5616fa9e39058e2ff9c664b0a7db141a9))

### Fix

* Fhirclient - parse release version issue ([`d992615`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/d9926152109df6f63397d3b2df639c07ee6daff0))

## v0.4.2 (2023-06-19)

### Fix

* Upgrade python dependencies ([`b5add22`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/b5add228760f1c7e22b93e79fb3db1e0e6d25c6c))
* Fhirtransformer template issues ([`4f720f4`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/4f720f4ab7f810af5a45e645326dd670478451ac))
* Dummy commit to release fhirtransformer ([`4210c7a`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/4210c7a9b67860ddcd99c532b8372276a4f3bc33))

## v0.4.1 (2023-06-12)

### Fix

* Fhirtransformer dependency ([`69cb5ce`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/69cb5cef6fe79153f85de853ac0b53cfc2b92d38))
* Remove pyspark from fhirtransformer ([`706750a`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/706750a9853d9861b5b8ef538349cd011f03eaa9))

## v0.4.0 (2023-05-29)
### Feature

* Added schema mapping for nucelo ([`7a5781e`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/7a5781e8ffca2d3fc3c6bf17d6986a84e982d517))

## v0.3.0 (2023-05-26)
### Feature
* Added Gi and Gap omniparser updates ([`522e239`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/522e239f16f286aeeb9e920e3f499e72aa8ccc86))

### Fix
* Package release issues ([`4bbbfda`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/4bbbfda67eb472d790b13609d538b83e57d3fc22))
* Fhirtransfer template issues for ncqa ([`c012d82`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/c012d82bc864869703be8f2e140b7d0d8b78a455))

## v0.2.3 (2023-05-19)
### Fix
* Add organization, practitioner scope support ([`662f612`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/662f6127ccaea8eecf0d1290e502abefa8248319))

## v0.2.2 (2023-05-12)
### Fix
* Reduce omniparser line processing delay time ([`e36288b`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/e36288bcae4a3959d6bd91bc8572b7cb7d1c11a3))

## v0.2.1 (2023-04-19)
### Fix
* Update package build approach with poetry build ([`6a99cba`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/6a99cba7d7edae00d9d7f2601e85afa352e7f059))
* Investigate package registry Bad Request issue ([`34bae12`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/34bae12865c3752498ac228a7b20adece8ddebcb))

## v0.2.0 (2023-04-19)
### Feature
* Add version upgrade trigger for versioned packages ([`901a0a8`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/901a0a810805ea6f7bc11169858f56ccda97ea52))

## v0.1.0 (2023-04-18)
### Feature
* Added omniparser config for baco hap, humana payers ([`94c9844`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/94c98442049e7d092a38def014c267e680441e9d))
* Added templates in practitioner resource for migration jobs ([`49d0368`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/49d0368a52f769f29e8f969bd631bbb24b9b3b03))
* Added baco schema mapping and schema clean up ([`d452d86`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/d452d867cc803e5f3511e937af08aef17d8e8bfb))
* Updated json validator code ([`41fb976`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/41fb9764cf45baed00dd7c5f5ecec6531e56c29b))
* Update omniparser tenant schema mapping ([`fa2d653`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/fa2d65376a9cf1d1984f3bbe7d0a382ec1338a5c))
* Added poetry for ETL libraries ([`43f8081`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/43f80814d644567c1247d9c050a88ba55a11241d))

### Fix
* Fhirclient match response issue ([`ff35f77`](https://gitlab.com/health-ec/platform/core/libraries/etl-libraries/-/commit/ff35f77a116675a21d11c7a09db7f7af23466020))

## v1.1.0 (2023-02-23)
### Feature
* Ecw - PHM-25099 ([`eab8fb6`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/eab8fb68be3f8a37837911c1cfc582f15655485c))
* Omniparser config for hap medical claim ([`69da4c0`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/69da4c041541d7fd74a9d2ba63001cd041f313ce))
* Update ECW Observation (Vital) omniparser config ([`4bd0ef0`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/4bd0ef04fb5e814a51de246034a1edbd826431af))
* Ecw encounter,patient_demographics,procedure Schemas and constants changes added ([`1c69123`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/1c69123b21da744b13b23b269437829cd40103f9))
* Added jinja template for insurance plan FHIR resource ([`a2e7c15`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/a2e7c1542f9132cb93e29acd6c315567a2e4a6ab))
* Update fhirtransformer resources - PHM-25096 ([`6318413`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/6318413e8f3e37ee2c3229fff1f54dc2b69e603c))
* Fhirtransformer - added coverage resource - PHM-24720 ([`119318f`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/119318fc24b86cae184b817970e86f0b140b1f5d))
* Add organization affiliation - PHM-24721 ([`d90e4b1`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/d90e4b14e43b5cb685ed820bce128518d767b838))
* Update omniparser config - PHM-24282 ([`295b516`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/295b516675638bfc80ccc64ed390c4f70a765ded))
* Set up pytest for omniparser - PHM-24555 ([`fa6d374`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/fa6d374327d346a91faadf7617f89e6076ee96fe))
* Adding BACO_practice_and_provider Config ([`25ef2ff`](https://github.com/health-ec/architecture/prototypes/etl/libraries/commit/25ef2ff83ae0eba4b4027aeccdf60ac3877c3ae6))
