from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import random

ACCOUNT_TYPES = ("checking", "savings", "wallet")
ACCOUNT_STATUSES = ("active", "active", "active", "suspended")
TRANSACTION_TYPES = ("card_purchase", "cash_withdrawal", "deposit", "fee")
TRANSACTION_STATUSES = ("completed", "completed", "pending", "failed")
TRANSFER_STATUSES = ("completed", "completed", "pending", "failed")
PAYMENT_STATUSES = ("completed", "completed", "pending", "failed")
MERCHANTS = (
    ("Mercado Local", "groceries"),
    ("CloudStream", "subscription"),
    ("RideHop", "transport"),
    ("PowerGrid", "utilities"),
    ("ByteMart", "electronics"),
)


@dataclass(frozen=True)
class SeedBundle:
    accounts: list[tuple]
    transactions: list[tuple]
    transfers: list[tuple]
    payments: list[tuple]


def build_seed_bundle(
    *,
    seed: int,
    account_count: int,
    transaction_count: int,
    transfer_count: int,
    payment_count: int,
) -> SeedBundle:
    rng = random.Random(seed)
    base_time = datetime(2026, 1, 1, 9, 0, 0)

    accounts = build_accounts(rng, base_time, account_count)
    account_ids = [account[0] for account in accounts]
    transactions = build_transactions(rng, base_time, transaction_count, account_ids)
    transfers = build_transfers(rng, base_time, transfer_count, account_ids)
    payments = build_payments(rng, base_time, payment_count, account_ids)
    return SeedBundle(
        accounts=accounts,
        transactions=transactions,
        transfers=transfers,
        payments=payments,
    )


def build_accounts(rng: random.Random, base_time: datetime, count: int) -> list[tuple]:
    accounts = []
    for offset in range(count):
        account_id = 1000 + offset
        created_at = base_time + timedelta(minutes=offset * 11)
        balance = Decimal(str(round(rng.uniform(150.0, 15000.0), 2)))
        accounts.append(
            (
                account_id,
                f"CUST-{account_id:06d}",
                rng.choice(ACCOUNT_TYPES),
                rng.choice(ACCOUNT_STATUSES),
                "USD",
                balance,
                created_at,
                created_at,
            )
        )
    return accounts


def build_transactions(
    rng: random.Random,
    base_time: datetime,
    count: int,
    account_ids: list[int],
) -> list[tuple]:
    transactions = []
    for offset in range(count):
        created_at = base_time + timedelta(minutes=offset * 7)
        amount = Decimal(str(round(rng.uniform(5.0, 1200.0), 2)))
        transaction_type = rng.choice(TRANSACTION_TYPES)
        transactions.append(
            (
                5000 + offset,
                rng.choice(account_ids),
                transaction_type,
                amount,
                rng.choice(TRANSACTION_STATUSES),
                f"{transaction_type.replace('_', ' ')} #{offset + 1}",
                created_at,
                created_at + timedelta(minutes=rng.randint(0, 120)),
            )
        )
    return transactions


def build_transfers(
    rng: random.Random,
    base_time: datetime,
    count: int,
    account_ids: list[int],
) -> list[tuple]:
    transfers = []
    for offset in range(count):
        created_at = base_time + timedelta(minutes=offset * 13)
        source_account_id = rng.choice(account_ids)
        destination_account_id = rng.choice(account_ids)
        while destination_account_id == source_account_id:
            destination_account_id = rng.choice(account_ids)
        transfers.append(
            (
                8000 + offset,
                source_account_id,
                destination_account_id,
                Decimal(str(round(rng.uniform(20.0, 2500.0), 2))),
                rng.choice(TRANSFER_STATUSES),
                created_at,
                created_at + timedelta(minutes=rng.randint(0, 180)),
            )
        )
    return transfers


def build_payments(
    rng: random.Random,
    base_time: datetime,
    count: int,
    account_ids: list[int],
) -> list[tuple]:
    payments = []
    for offset in range(count):
        created_at = base_time + timedelta(minutes=offset * 17)
        merchant_name, category = rng.choice(MERCHANTS)
        payments.append(
            (
                9000 + offset,
                rng.choice(account_ids),
                merchant_name,
                category,
                Decimal(str(round(rng.uniform(10.0, 900.0), 2))),
                rng.choice(PAYMENT_STATUSES),
                created_at,
                created_at + timedelta(minutes=rng.randint(0, 60)),
            )
        )
    return payments
