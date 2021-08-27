import unittest
from ntropy_sdk import Transaction


class TestSDK(unittest.TestCase):
    def test_transaction_entry_type(self):
        for et in ["incoming", "outgoing", "debit", "credit"]:
            t = Transaction(
                amount=1.0, description="foo", entry_type=et, entity_id="bar"
            )

        self.assertRaises(
            ValueError,
            lambda: Transaction(
                amount=1.0,
                description="foo",
                entry_type="bar",
                entity_id="bar",
            ),
        )
