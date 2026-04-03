from __future__ import annotations

import argparse

from oltp_warehouse.generator import BootstrapConfig, bootstrap_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="oltp-warehouse",
        description="Bootstrap a synthetic PostgreSQL OLTP dataset.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Drop and recreate the demo schema, then seed synthetic data.",
    )
    bootstrap.add_argument("--seed", type=int, default=42, help="Random seed.")
    bootstrap.add_argument(
        "--accounts",
        type=int,
        default=25,
        help="Number of accounts to create.",
    )
    bootstrap.add_argument(
        "--transactions",
        type=int,
        default=120,
        help="Number of transactions to create.",
    )
    bootstrap.add_argument(
        "--transfers",
        type=int,
        default=40,
        help="Number of transfers to create.",
    )
    bootstrap.add_argument(
        "--payments",
        type=int,
        default=60,
        help="Number of payments to create.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "bootstrap":
        config = BootstrapConfig(
            seed=args.seed,
            accounts=args.accounts,
            transactions=args.transactions,
            transfers=args.transfers,
            payments=args.payments,
        )
        summary = bootstrap_database(config)
        print(
            "Bootstrapped synthetic OLTP data: "
            f"{summary['accounts']} accounts, "
            f"{summary['transactions']} transactions, "
            f"{summary['transfers']} transfers, "
            f"{summary['payments']} payments."
        )
        return 0

    parser.error("Unknown command")
    return 2
