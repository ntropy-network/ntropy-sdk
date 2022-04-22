# Ntropy SDK

This repository hosts the SDK for the Ntropy API.  To use the Ntropy API you require an API key which can be requested at [ntropy.com](https://ntropy.com).
The Ntropy API provides transaction enrichment and categorization, account ledger, metrics and custom model training.


## Installation:

```bash
$ python3 -m pip install --upgrade 'ntropy-sdk'
```

## SDK initialization:

You can initialize the SDK by passing an API token to the constructor, or using the environment variable `NTROPY_API_KEY`

```python
from ntropy_sdk import SDK, Transaction, AccountHolder


# provide the token on initialization
sdk = SDK("my-ntropy-api-token")
```

The SDK contains methods to explore the available labels and chart of accounts:

```python

for account_holder_type in ['business', 'consumer', 'freelance', 'unknown']:
  print("ACCOUNT HOLDER TYPE:", account_holder_type)
  print(sdk.get_labels(account_holder_type))

print("CHART of ACCOUNTS:", sdk.get_chart_of_accounts())

```

## Enrichment usage:

A transaction can be associated with an account holder. An account holder represents an entity (business, freelancer or consumer) that can be uniquely identified and holds the account associated with the transaction:

- In an outgoing (debit) transaction, the account holder is the sender and the merchant is the receiver.
- In an incoming (credit) transaction, the account holder is the receiver and the merchant is the sender.

Account holders provide important context for understanding transactions and allow additional operations such as extracting metrics for account holders over time.

```python

import os
import uuid
from datetime import datetime

account_holder = AccountHolder(
    id=str(uuid.uuid4()),
    type="business",
    name="Ntropy Network Inc.",
    industry="fintech",
    website="ntropy.com"
)
sdk.create_account_holder(account_holder)
```

A new transaction can then be associated with the created account holder and enriched:

```python
transaction = Transaction(
    amount=12046.15,
    date="2021-12-13",
    description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15",
    entry_type="outgoing",
    iso_currency_code="USD",
    account_holder_id=account_holder.id,
    country="US",
)

transaction_list = [transaction]
enriched_transactions = sdk.add_transactions(transaction_list)

# Alternatively, you can provide both account_holder_id and account_holder_type, and set create_account_holders to True
# The account holders are then dinamically created if they don't exist during the add_transactions API call
# enriched_transactions = sdk.add_transactions(transaction_list, create_account_holders=True)

print("ENRICHED:", enriched_transactions)
```

A transaction can also be submitted without associating it to an account holder by just providing the `account_holder_type`. If no `account_holder_type` and `account_holder_id` is provided for a transaction, the `labels` field of the enriched transaction will only contain the label "missing account holder information".

```python
transaction = Transaction(
    amount=12046.15,
    date="2021-12-13",
    description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15",
    entry_type="outgoing",
    iso_currency_code="USD",
    account_holder_type="consumer",
    country="US",
)

transaction_list = [transaction]
enriched_transactions = sdk.add_transactions(transaction_list)
```

The API can be queried for specific stats regarding account holders such as the total amount in transactions in a time interval:

```python

query = account_holder.get_metrics(['amount'], start=datetime.strptime("2021-12-01", "%Y-%m-%d"), end=datetime.strptime("2022-01-01", "%Y-%m-%d"))
print("QUERY:", query['amount'])

```

For bulk data, the API can easily be used along with some popular libraries such as `pandas` to facilitate the process:

```python
import pandas as pd

df = pd.DataFrame(data = [[12046.15, '2021-12-13', "AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15", 'outgoing', 'USD', 'US', str(uuid.uuid4()), 'business', "Ntropy Network Inc.", "fintech", "ntropy.com"]], columns = ["amount", "date", "description", "entry_type", "iso_currency_code", "country", "account_holder_id", "account_holder_type", "account_holder_name", "account_holder_industry", "account_holder_website"])

def create_account_holder(row):
    sdk.create_account_holder(
        AccountHolder(
            id=row["account_holder_id"],
            type=row["account_holder_type"],
            name=row.get("account_holder_name"),
            industry=row.get("account_holder_industry"),
            website=row.get("account_holder_website"),
        )
    )

df.groupby("account_holder_id", as_index=False).first().apply(create_account_holder, axis=1)

enriched_df = sdk.add_transactions(df)
print("ENRICHED:", enriched_df)

```

## Models usage:

Using the SDK you can train and run a custom model for transaction classification.
This custom model makes use of Ntropy's advanced base models and provides additional capabilities for customization and fine-tuning based on user provided labeled data.

If you're familiar with using scikit-learn, the usage for Ntropy models will be familiar. A full example:

```python
import pandas as pd
from ntropy_sdk.models import CustomTransactionClassifier

train_df = pd.read_csv('labeled_transactions.csv')
train_labels = transactions_df['label']

test_df = pd.read_csv('test_set_transactions.csv')
test_labels = test_set['label']

model = CustomTransactionClassifier('classifier-example')
model = model.fit(train_df, train_labels)

print(model.score(test_df, test_labels))

from sklearn.model_selection import cross_validate
print(cross_validate(model, train_df, train_labels))

import pickle
with open('model.pkl', 'wb') as fp:
    pickle.dump(model)
```

In the following sections we will go into more detail for each part of model usage.

### Loading data

To train a custom model you need to load a set of labeled transactions. Each transaction must have the following information: `amount`, `description`, `iso_currency_code`, `account_holder_type`, `entry_type` and the ground truth label

Assuming they are stored in a csv:

```python

# csv with columns ["amount", "description", "iso_currency_code", "account_holder_type", "entry_type", "label"]
train_df = pd.read_csv('labeled_transactions.csv')
train_labels = transactions_df['label']

```

### Initialization and fitting

The model interfaces provided in `ntropy_sdk.models package` are fully scikit-learn compatible (`Estimator` and `Classifier`). Each model will run in Ntropy's servers and not locally.

A model is initialized with a unique name. Instantiating a model with the same name will refer to the same stored model and not allow training if the model is already trained. If you intend to train the model again (overriding current stored model with that name) you can set `allow_retrain` to True when creating the model.

To train a model the process is the same as a scikit-learn classifier:

```python

from ntropy_sdk.models import CustomTransactionClassifier

# you can skip providing the sdk as long as you have the NTROPY_API_KEY environment variable set
model = CustomTransactionClassifier('example-classifier', allow_retrain=True, sdk=sdk)

# or you can set the sdk later
model.set_sdk(sdk)

model.fit(train_df, train_labels)
```

Training data can be provided as a dataframe, list of dictionaries or list of `ntropy_sdk.Transactions` as long as the required information is provided. You must provide at least 16 examples for each label during training, and the API curently supports fine-tuning on at most 8000 transactions.

### Prediction and scoring

The model can then be used to predict the label of new transactions:

```python

new_transactions = pd.read_csv('new_transactions.csv')
labels = model.predict(new_transactions)
```

By default you can score the model using a micro-averaged F1 score:

```python
test_df = pd.read_csv('test_set_transactions.csv')
test_labels = test_set['label']
print(model.score(test_df, test_labels))
```

You can also use the tools from scikit-learn and other compatible libraries for classifiers, such as cross_validate. However, you need to set `allow_retrain` to True when using methods that fit the model multiple times.

```python
from sklearn.model_selection import cross_validate

print(cross_validate(model, train_df, train_labels))
```

### Saving and loading

The SDK models can be stored in pickled format. However, the models are referenced by the provided name. If you instantiate a model with the same name as a previously trained model you will efectively load the previous model:

```python

# after the example-classifier was trained
trained_model = CustomTransactionClassifier('example-classifier', sdk=sdk)
output_labels = trained_model(test_set)
```

Serializing the model (i.e., pickling) will mainly store the name of the model for later.


## License:
Free software: MIT license


