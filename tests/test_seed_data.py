from oltp_warehouse.seed_data import build_seed_bundle


def test_seed_bundle_is_deterministic():
    bundle_a = build_seed_bundle(
        seed=42,
        account_count=3,
        transaction_count=4,
        transfer_count=2,
        payment_count=2,
    )
    bundle_b = build_seed_bundle(
        seed=42,
        account_count=3,
        transaction_count=4,
        transfer_count=2,
        payment_count=2,
    )

    assert bundle_a == bundle_b


def test_transfer_accounts_are_distinct():
    bundle = build_seed_bundle(
        seed=7,
        account_count=5,
        transaction_count=0,
        transfer_count=10,
        payment_count=0,
    )

    assert bundle.transfers
    assert all(source != destination for _, source, destination, *_ in bundle.transfers)
