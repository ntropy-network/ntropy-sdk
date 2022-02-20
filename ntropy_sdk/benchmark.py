import sys
import argparse
import ast
from typing import List, Dict

from ntropy_sdk import SDK


def limited_type(field, min, max):
    def _lim(x):
        x = int(x)
        if x < min:
            raise argparse.ArgumentTypeError(f"Minimum value for {field} is {min}")
        if x > max:
            raise argparse.ArgumentTypeError(f"Maximum value for {field} is {max}")
        return x

    return _lim


parser = argparse.ArgumentParser(description="Benchmark the results of a CSV")
parser.add_argument("--api-key", required=True, type=str, help="api key to use")
parser.add_argument(
    "--api-url", type=str, default="https://api.ntropy.com", help="API url"
)
parser.add_argument(
    "--in-csv-file",
    type=str,
    required=True,
    help="Input file to read transactions from",
)
parser.add_argument(
    "--out-csv-file",
    type=str,
    help="Output file to write enriched transactions to (if empty, no file will be written)",
)

parser.add_argument(
    "--drop-fields",
    type=str,
    help="Any fields to ignore from the CSV input file (comma-separated)",
)
parser.add_argument(
    "--hardcoded-fields",
    type=str,
    help="Any fields to hardcoded in the CSV input file (python dictionary formatting)",
)

parser.add_argument(
    "--field-mapping",
    type=str,
    help="Any fields to map in the output CSV file. The default mapping is usually sufficient.",
)

parser.add_argument(
    "--max-batch-size",
    type=limited_type("max-batch-size", 1, 100000),
    default=100000,
    help="The maximum size for batches. Defaults to 100000 (which is also the maximum batch size)",
)

parser.add_argument(
    "--poll-interval",
    type=limited_type("poll-interval", 1, 10),
    default=10,
    help="The interval at which we check for new results. Defaults to 10 seconds",
)

parser.add_argument(
    "--ground-truth-merchant-field",
    type=str,
    help="The field in the input csv containing the correct merchant (if unset no score will be calculated)",
)

parser.add_argument(
    "--ground-truth-label-field",
    type=str,
    help="The field in the input csv containing the correct label (if unset no score will be calculated)",
)


def _get_nodes(x, prefix=""):
    """
    Args:
        x: a tree where internal nodes are dictionaries, and leaves are lists.
        prefix: not meant to be passed. The parent prefix of a label. e.g. given A -> B -> C,
            the parent prefix of C is 'A [sep] B'.
        sep: the separator to use between labels. Could be 'and', '-', or whatever
    Returns:
        All nodes in the hierarchy. Each node is given by a string A [sep] B [sep] C etc.
    """
    res = []
    q = [(x, prefix)]
    while q:
        x, prefix = q.pop()
        if isinstance(x, list):
            res.extend([prefix + k for k in x])
        else:
            for k, v in x.items():
                res.append(prefix + k)
                q.append((v, prefix + k + " - "))
    return list(set(res))


def _node2branch(branch):
    if isinstance(branch, str):
        branch = branch.split(" - ")
    return [" - ".join(branch[: i + 1]) for i in range(len(branch))]


def benchmark(
    sdk,
    in_csv_file: str,
    out_csv_file: str,
    drop_fields: List[str] = None,
    hardcode_fields: Dict[str, str] = None,
    ground_truth_merchant_field=None,
    ground_truth_label_field=None,
    mapping=None,
    poll_interval=10,
):
    try:
        import pandas as pd
    except ImportError:
        print(
            "Pandas not found, please install ntropy-sdk with the benchmark extra"
            " (e.g. pip install 'ntropy-sdk[benchamrk]') to use the benchmarking functionality"
        )
        sys.exit(1)
    try:
        import numpy as np
    except ImportError:
        print(
            "Numpy not found, please install ntropy-sdk with the benchmark extra"
            " (e.g. pip install 'ntropy-sdk[benchamrk]') to use the benchmarking functionality"
        )
        sys.exit(1)
    try:
        from sklearn.metrics import precision_recall_fscore_support
    except ImportError:
        print(
            "Scikit-learn not found, please install ntropy-sdk with the benchmark extra"
            " (e.g. pip install 'ntropy-sdk[benchamrk]') to use the benchmarking functionality"
        )
        sys.exit(1)
    default_mapping = sdk.DEFAULT_MAPPING.copy()
    if mapping is not None:
        default_mapping.update(mapping)
    mapping = default_mapping

    df = pd.read_csv(in_csv_file)
    if drop_fields:
        df = df.drop(drop_fields, axis=1)
    if hardcode_fields:
        for a, b in hardcode_fields.items():
            df[a] = b
    df = sdk.add_transactions(df, mapping=mapping, poll_interval=poll_interval)
    if ground_truth_merchant_field:
        correct_merchants = df[ground_truth_merchant_field]
        predicted_merchants = df[mapping["merchant"]]
        accuracy_merchant = np.mean(
            [
                x == y
                for x, y in zip(
                    correct_merchants,
                    predicted_merchants,
                )
            ]
        )
        output = f"Merchant:\n\tAccuracy: {accuracy_merchant:.3f}%"
        print(output)
    if ground_truth_label_field:
        labels_per_type = {
            "consumer": _get_nodes(sdk.get_labels("consumer")),
            "business": _get_nodes(sdk.get_labels("business")),
        }

        correct_labels = df[ground_truth_label_field].to_list()
        account_holder_types = [t for t in df["account_holder_type"].to_list()]
        predicted_labels = df[mapping["labels"]].to_list()
        y_pred = []
        y_true = []
        for x, y, account_holder_type in zip(
            correct_labels, predicted_labels, account_holder_types
        ):
            nodes = labels_per_type[account_holder_type]
            ground_truth = _node2branch(x)
            preds = _node2branch(y)
            for node in nodes:
                y_true.append(node in ground_truth)
                y_pred.append(node in preds)
        labeller_accuracy = np.mean(
            [x == " - ".join(y) for x, y in zip(correct_labels, predicted_labels)]
        )
        (
            precision_labeller,
            recall_labeller,
            f1_labeller,
            _,
        ) = precision_recall_fscore_support(
            y_true, y_pred, average="binary", zero_division=0.0
        )

        output = (
            "Labels:\n"
            f"\tF1: {f1_labeller:.3f}\n"
            f"\tPrecision: {precision_labeller:.3f}\n"
            f"\tRecall: {recall_labeller:.3f}\n"
            f"\tAccuracy: {labeller_accuracy:.3f}\n"
        )
        print(output)
    if out_csv_file:
        df.to_csv(out_csv_file)


def main():
    args = parser.parse_args()
    sdk = SDK(args.api_key)
    benchmark(
        sdk,
        args.in_csv_file,
        args.out_csv_file,
        drop_fields=args.drop_fields.split(",") if args.drop_fields else [],
        hardcode_fields=ast.literal_eval(args.hardcoded_fields)
        if (args.hardcoded_fields)
        else None,
        mapping=ast.literal_eval(args.field_mapping) if args.field_mapping else None,
        ground_truth_merchant_field=args.ground_truth_merchant_field,
        ground_truth_label_field=args.ground_truth_label_field,
        poll_interval=args.poll_interval,
    )
