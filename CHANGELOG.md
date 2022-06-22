
# Changelog
All notable changes to this project will be documented in this file.

## [Unmerged] 
- Update get_account_holder to return an AccountHolder object
- Fix EnrichedTransaction report system

## [4.6.0] – 2022-06-16
- Allow zero amount transactions, removed flag
- Progress bar enabled by default for only interactive mode

## [4.5.0] – 2022-06-10 
- Correctly handle ConnectionError when acessing API for expired HTTP sessions. Requests are retried by creating a new requests Session.
- Disabled zero amount check by default
- Account holder id is no longer mandatory for enrichments using dataframes
- Added missing mcc field when using dataframes
- Added `unique_merchant_id` field

## [4.4.21] - 2022-04-13
- Updated documentation

## [4.4.1] - 2022-04-13

- Added `inplace` flag for `add_transactions` that is used when passing a dataframe to ensure if the input dataframe is modified or not
- Extended support for older Python versions starting from 3.6 and upwards
- Set default value for `create_account_holders` in `add_transactions` to `True`

## [4.4.0] - 2022-04-06
### Added
- Made `account_holder_id` no longer required (but still usable). Transactions can now be enriched by only providing `account_holder_type`, and will not be tied to the ledger of any `account_holder`. Transactions can also be sent without `account_holder_id` and without `account_holder_type` but will not be labeled.
- Added `transaction_type` attribute to the `EnrichedTransaction` object which contains the expected type of expense for the enriched transaction (`business`, `consumer` or `unknown`)
- Add models package to integrate with the new custom fine-tuning service. Includes `CustomTransactionClassifier` as a trainable model, fully compatible with scikit-learn Estimator and Classifier interface.
- Allow initialization of the SDK using the environment variable `NTROPY_API_KEY`
- Removed `benchmark.py` file and corresponding documentation


## [4.3.0] - 2022-03-17
### Added
- `create_account_holders` parameter for `SDK.add_transactions`. When set to `True`, if a `Transaction` contains both an `account_holder_id` and `an account_holder_type`, that account holder will be created during the `add_transactions` method.
- `SDK.get_account_holder` method to retrieve, if it exists, stored information about one account holder.
- `confidence` attribute to the `EnrichedTransaction` object.
