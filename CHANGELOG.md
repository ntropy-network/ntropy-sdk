
# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

- Add models package. Includes `FewShotClassifier` as the first trainable model, fully compatible with scikit-learn Estimator and Classifier interface.
- Allow initialization of the SDK from environment variable `NTROPY_API_KEY`
- Removed `benchmark.py` file and corresponding documentation

## [4.3.0] - 2022-03-17
### Added
- `create_account_holders` parameter for `SDK.add_transactions`. When set to `True`, if a `Transaction` contains both an `account_holder_id` and `an account_holder_type`, that account holder will be created during the `add_transactions` method.
- `SDK.get_account_holder` method to retrieve, if it exists, stored information about one account holder.
- `confidence` attribute for the `EnrichedTransaction` object.
