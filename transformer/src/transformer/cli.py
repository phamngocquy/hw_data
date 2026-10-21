import argparse

from transformer.core.runner import run
from transformer.registry import TRANSFORMERS


def main() -> None:
    parser = argparse.ArgumentParser(prog="transformer")
    parser.add_argument("source", choices=sorted(TRANSFORMERS))
    parser.add_argument(
        "--input",
        required=True,
        help="raw file: a versioned .vN.json or its frontier symlink",
    )
    parser.add_argument(
        "--window-end",
        required=True,
        help='window end hour, "YYYYMMDD HH" (UTC); names the output file',
    )
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    result = run(
        TRANSFORMERS[args.source],
        args.input,
        args.window_end,
        run_id=args.run_id,
    )
    print(result["output_file"])
