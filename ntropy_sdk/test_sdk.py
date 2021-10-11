import unittest
from ntropy_sdk import Transaction


class TestSDK(unittest.TestCase):
    def test_transaction_zero_amount(self):
        vals = {
            "description": "foo",
            "entry_type": "debit",
            "account_holder_id": "1",
            "country": "US",
        }

        testcases = [
            (0, True, True),
            (0, False, False),
            (-1, True, True),
            (-1, False, True),
            (1, False, False),
            (1, True, False),
        ]
        for amount, enabled, should_raise in testcases:
            print("Testcase:", amount, enabled, should_raise)
            if enabled:
                Transaction.enable_zero_amount_check()
            else:
                Transaction.disable_zero_amount_check()
            if should_raise:
                self.assertRaises(
                    ValueError,
                    Transaction,
                    amount=amount,
                    **vals,
                )
            else:
                Transaction(
                    amount=amount,
                    **vals,
                )
        Transaction.enable_zero_amount_check()

    def test_transaction_entry_type(self):
        for et in ["incoming", "outgoing", "debit", "credit"]:
            t = Transaction(
                amount=1.0, description="foo", entry_type=et, account_holder_id="bar", country="US"
            )

        self.assertRaises(
            ValueError,
            lambda: Transaction(
                amount=1.0,
                description="foo",
                entry_type="bar",
                account_holder_id="bar",
                country="FOO"
            ),
        )
