import argparse
import ast
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
    "--api-url", type=str, default="https://api.ntropy.network", help="API url"
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


def benchmark():
    args = parser.parse_args()
    s = SDK(args.api_key)
    s.benchmark(
        args.in_csv_file,
        args.out_csv_file,
        drop_fields=args.drop_fields.split(",") if args.drop_fields else [],
        hardcode_fields=ast.literal_eval(args.hardcoded_fields),
        mapping=ast.literal_eval(args.field_mapping) if args.field_mapping else None,
        ground_truth_merchant_field=args.ground_truth_merchant_field,
        ground_truth_label_field=args.ground_truth_label_field,
        chunk_size=args.max_batch_size,
        poll_interval=args.poll_interval,
    )
