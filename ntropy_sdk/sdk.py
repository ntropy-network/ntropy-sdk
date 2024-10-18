from ntropy_sdk.http import HttpClient
from ntropy_sdk.transactions import TransactionsResource
from ntropy_sdk.batches import BatchesResource
from ntropy_sdk.bank_statements import BankStatementsResource
from ntropy_sdk.account_holders import AccountHoldersResource


class SDK:
    def __init__(self):
        self.http_client = HttpClient()
        self.transactions = TransactionsResource(self)
        self.batches = BatchesResource(self)
        self.bank_statements = BankStatementsResource(self)
        self.account_holders = AccountHoldersResource(self)
