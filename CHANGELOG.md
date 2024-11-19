
# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
- Add `AsyncSDK` and corresponding async methods

## [5.0.2] - 2024-11-07
- Add `transaction_ids` to recurring groups
- Add progress to `sdk.batches.wait_for_result`

## [5.0.1] - 2024-11-06
- Integration with v3 version of the api
- API resources are under `SDK` and consistently organized into `create`, `get`, `list` and `delete`.
```python
sdk.transactions.create(...)
sdk.account_holders.create(...)
```
- Auto-pagination is now supported for listing resources
```python
for txn in sdk.transactions.list().auto_paginate():
    ...
```
- Request ID integration for easier debugging
- Easier session lifecycle management through `SDK(session=http_session)`
- Added support for new resources: `webhooks`
- Added support for missing resources: `account_holders`, `entities`, `rules`, `categories`

## [4.20.0] - 2023-08-21
- Added `account_holder_id` property to bank statements structure

## [4.20.0] - 2023-08-21
- Added ability to specify account holder information to bank statements via `sdk.add_bank_statement`.

## [4.19.0]
- Added `google_maps_url` and `apple_maps_url` to the structured location.
- Improved bank statements waiting statuses and default file naming
- Removed deprecated `mcc` property
- Updated bank_statements to match new API definition
- Fixed pydantic version to 1.x

## [4.18.0]
- Added structured location support (address, city, state, country, postcode, longitude, store_number)

## [4.18.0]
- Reverted some small changes that introduced latency issues in small batches

## [4.17.0]
- Improved support for single transaction batches

## [4.16.0] - 2023-02-27
- Added a flag to SDK `raise_on_enrichment_error` that allows for enrichment to continue even if there are errors in
enrichment process by populating `error` and `error_details` of the affected transactions instead.

## [4.15.0] - 2023-02-08
- Added a method to SDK `sdk.add_bank_statement` to allow enrichment to be
performed on file data.
- For more details, refer to the relevant [documentation](https://developers.ntropy.com/docs/enrichment/#enriching-file-data).

## [4.14.1] - 2022-09-16
- Add an optional `transaction_ids` filter to `ntropy_sdk.get_account_holder_transactions` to only fetch transactions that match the given IDs.

## [4.14.0] 2022-12-15
- Added support for webhooks in report transaction endpoint and added new `sdk.report_transaction` method to report from base SDK object.
- Added support for new list report and get report endpoints.
- Update batch size limit to 24960
- Add `label_group` attribute for EnrichedTransaction

## [4.12.0] 2022-11-10
- Added warning related to non-unique transaction ids.
- Remove `chart_of_accounts` property from transactions
- Added deprecation warnings to soon-to-be deprecated arguments (`labeling`, `create_account_holders` and `model`)
- Removed use of `singledispatchmethod` in favour of simpler input type handling
- Improved error handling and parseability with several new error types (see ntropy_sdk.errors), for both client side and server side errors
- Added `intermediaries` to the EnrichedTransaction class containing the information for transaction intermediaries (i.e., payment processor)
- Added `created_at` property to `EnrichedTransaction`
- Added `region` parameter to SDK to allow connecting the SDK to EU datacenter seamlessly

## [4.11.0]  2022-10-13
- Added `ntropy_sdk.recurring_payments` package with support for recurring payments feature
- Improve repr of objects across the board, using pandas if available
- Added `ntropy_sdk.get_account_holder_transactions` to fetch all transactions for an account holder

## [4.10.1] - 2022-09-16
- Fixed support for Python 3.6
- Added RecurrenceGroup and recurrence_group for correct parsing of this information
- Rename `IncomeReport.report()` to `IncomeReport.report_dataframe()`

## [4.10.0] - 2022-08-31
- Added income checking to Ntropy SDK

## [4.9.0] - 2022-08-16
- Allow subnormal float values in pydantic fields
- Fix bug with inplace=False for dataframe enrichment
- Added tests for models module
- Updated scikit-compatible model to use SDK Model class
- Include original Transaction object in EnrichedTransaction
- Support iterables in `add_transactions` and `add_transactions_async`
- Ensure that `to_dict()` method always returns values even if None, but only returned fields that the API returns
- Added support for income checking API (IncomeReport, IncomeGroup)

## [4.8.3] - 2022-08-05
- Add repeating label to recurrence

## [4.8.2] - 2022-08-01
- Renamed `predicted_mcc` to `mcc` for simplicity.
- Increased mcc range to 700-9999

## [4.8.1] - 2022-07-22
- Fixed a bug with deprecated `model` parameter for custom models

## [4.8.0] – 2022-07-21
- Added TCP keep-alive to Ntropy API requests
- Migrated SDK classes to Pydantic (support for Decimal and str in numerical fields)
- Added new predicted_mcc field in EnrichedTransaction

## [4.7.0]  - 2022-07-04
- Update `get_account_holder` method to return an AccountHolder object instead of dictionaries
- Added docstrings to all public SDK methods. Consult the reference at https://developers.ntropy.com/sdk
- Added `retries` attribute to SDK object, which determines the number of retries per request before failling
- Added `retry_on_unhandled_exception` to SDK object, which controls whether the SDK should retry requests that return an error code in the range 500-511
- Removed `list_models` method. Added base custom model API to SDK that closer resembles the REST API structure. Added three new methods to SDK `train_custom_model`, `get_custom_model` and `get_all_custom_models`. Added new classes to represent custom model structures `LabeledTransaction` and `Model`.

## [4.6.0] – 2022-06-16
- Allow zero amount transactions, removed flag
- Progress bar enabled by default for only interactive mode

## [4.5.0] – 2022-06-10
- Correctly handle ConnectionError when accessing API for expired HTTP sessions. Requests are retried by creating a new requests Session.
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
