
# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

## [4.3.0] - 2022-03-17
### Added
- New `create_account_holders` parameter for `SDK.add_transactions`. When set to `True`, if a `Transaction` contains both an `account_holder_id` and `an account_holder_type`, that account holder will be created during the `add_transactions` method.
- New `SDK.get_account_holder` method to retrieve, if it exists, stored information about one account holder.
- New `confidence` attribute for the `EnrichedTransaction` object.