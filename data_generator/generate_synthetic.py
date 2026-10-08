#!/usr/bin/env python3
"""Synthetic e-commerce data generator (Phase 4).

Generates a deterministic, seedable e-commerce dataset as Parquet files for
the lakehouse pipeline. Downstream, Phase 5 bronze assets load these files
into Iceberg tables on Garage.

Entities
--------
customers, products, orders, order_items, payments, refunds,
web_events, support_tickets

Determinism
-----------
A master PCG64 generator seeded with ``--seed`` (default: DATA_SEED from .env)
spawns one child RNG per entity, so each table's data depends only on the
seed - never on generation order or on the size of other tables. Same seed
=> same rows, always (content hashes are recorded in manifest.json).

Business rules baked in (things to clean up / exploit in Silver & Gold):
- Some customers are heavy buyers (Zipf-like popularity).
- Order volume has a Nov/Dec holiday bump.
- Cancelled orders carry a failed payment; returned orders carry a refund.
- ~30% of web events are anonymous (null customer_id).
- Refunds can land after the data date range (realistic lag).

Usage
-----
    python data_generator/generate_synthetic.py
    python data_generator/generate_synthetic.py --seed 7 --num-orders 10000 \
        --out-dir data/synthetic

All parameters default from the repo-root .env (DATA_* variables) unless
overridden on the command line.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GeneratorConfig:
    """All knobs for one generation run (defaults from .env DATA_* vars)."""

    seed: int = int(os.environ.get("DATA_SEED", "42"))
    date_start: str = os.environ.get("DATA_DATE_START", "2024-01-01")
    date_end: str = os.environ.get("DATA_DATE_END", "2024-12-31")
    num_customers: int = int(os.environ.get("DATA_NUM_CUSTOMERS", "5000"))
    num_orders: int = int(os.environ.get("DATA_NUM_ORDERS", "50000"))
    num_products: int = 500
    num_web_events: int = 50_000
    num_support_tickets: int = 300


# ---------------------------------------------------------------------------
# Static vocabularies (small, local, no external data)
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "Lucas", "Mia", "Oliver", "Amelia", "Elijah", "Harper", "James",
    "Evelyn", "Benjamin", "Abigail", "Henry", "Ella", "Alexander", "Scarlett",
    "Sebastian", "Grace", "Jack", "Chloe", "Daniel", "Layla", "Owen", "Zoe",
    "Ryan", "Nora", "Nathan", "Lily", "Samuel", "Hannah", "David", "Aria",
    "Joseph", "Aria", "Carter", "Maya", "Wyatt", "Riley", "John", "Aurora",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
]
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "example.com"]
COUNTRIES = [
    ("United States", ["Austin", "Seattle", "Chicago", "Denver", "Boston", "Miami", "Portland"]),
    ("Canada", ["Toronto", "Vancouver", "Montreal"]),
    ("United Kingdom", ["London", "Manchester", "Birmingham"]),
    ("Germany", ["Berlin", "Munich", "Hamburg"]),
    ("France", ["Paris", "Lyon", "Bordeaux"]),
    ("Netherlands", ["Amsterdam", "Utrecht", "Rotterdam"]),
]
MARKETING_CHANNELS = [
    "organic_search", "paid_search", "email", "social", "referral", "direct",
]
# (category, brand pool, median price in dollars, lognormal sigma on log scale)
PRODUCT_CATALOG: list[tuple[str, list[str], float, float]] = [
    ("Electronics", ["Voltex", "Nimbus", "Circuitry"], 120.0, 0.9),
    ("Clothing", ["Nordwind", "UrbanThread", "Loom&Co"], 45.0, 0.7),
    ("Home & Kitchen", ["Hearthside", "Copperline"], 60.0, 0.8),
    ("Beauty", ["Velvette", "PureDew"], 28.0, 0.6),
    ("Sports & Outdoors", ["TrailForge", "PeakPro"], 70.0, 0.7),
    ("Toys & Games", ["PlayWright", "BlockBuster"], 30.0, 0.6),
    ("Books & Media", ["PaperTrail"], 22.0, 0.5),
    ("Grocery", ["HarvestBox", "DailyCart"], 8.0, 0.5),
]
PRODUCT_NOUNS = [
    "Widget", "Gadget", "Essentials", "Pro Kit", "Classic", "Deluxe",
    "Starter Set", "Premium", "Lite", "Max",
]
ORDER_STATUSES = ["completed", "returned", "cancelled"]
ORDER_STATUS_P = [0.84, 0.07, 0.09]
CHANNELS = ["web", "mobile", "pos"]
CHANNEL_P = [0.55, 0.35, 0.10]
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay", "gift_card", "bank_transfer"]
PAYMENT_METHOD_P = [0.55, 0.20, 0.12, 0.08, 0.05]
REFUND_REASONS = ["damaged", "wrong_item", "changed_mind", "late_delivery", "other"]
REFUND_REASON_P = [0.30, 0.20, 0.25, 0.15, 0.10]
EVENT_TYPES = ["page_view", "search", "add_to_cart", "checkout", "purchase", "wishlist"]
EVENT_TYPE_P = [0.55, 0.12, 0.12, 0.10, 0.06, 0.05]
DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_P = [0.45, 0.48, 0.07]
ISSUE_TYPES = ["shipping", "damaged", "wrong_item", "billing", "product_question", "other"]
ISSUE_TYPE_P = [0.30, 0.20, 0.15, 0.15, 0.10, 0.10]
TICKET_STATUSES = ["resolved", "open", "pending"]
TICKET_STATUS_P = [0.60, 0.25, 0.15]
TICKET_PRIORITIES = ["low", "medium", "high", "urgent"]
TICKET_PRIORITY_P = [0.40, 0.40, 0.15, 0.05]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _child_rngs(seed: int, count: int) -> list[np.random.Generator]:
    """Spawn independent child RNGs from a master seed (order-stable)."""
    master = np.random.SeedSequence(seed)
    return [np.random.default_rng(ss) for ss in master.spawn(count)]


def _dates_between(start: date, end: date) -> np.ndarray:
    """All calendar days from start to end (inclusive) as datetime64[D]."""
    n = (end - start).days + 1
    base = np.datetime64(start.isoformat(), "D")
    return base + np.arange(n, dtype="int64").astype("timedelta64[D]")


def _dates_to_list(arr: np.ndarray) -> list[date]:
    """numpy datetime64[D] -> python date list (polars Date dtype)."""
    return arr.astype(object).tolist()


def _seasonal_day_weights(day: np.ndarray) -> np.ndarray:
    """Holiday-season order bump: Nov x1.4, Dec x1.9 (relative to base 1.0)."""
    month = day.astype("datetime64[M]").astype(int) % 100 + 1
    weights = np.ones(len(day), dtype=np.float64)
    weights[month == 11] = 1.4
    weights[month == 12] = 1.9
    return weights / weights.sum()


def _sha256_of_frame(df: pl.DataFrame) -> str:
    """Stable content hash of a DataFrame (identical rows => identical hash)."""
    return hashlib.sha256(df.serialize()).hexdigest()


def _dtypes_of(df: pl.DataFrame) -> dict[str, str]:
    return {name: str(dtype) for name, dtype in df.schema.items()}


# ---------------------------------------------------------------------------
# Entity generators
# ---------------------------------------------------------------------------


def generate_products(cfg: GeneratorConfig, rng: np.random.Generator) -> pl.DataFrame:
    """Products: 500 items across 8 categories with category-based prices.

    Prices are lognormal around the category's median (mu is on the log
    scale: rng.lognormal(log(median), sigma)).
    """
    n = cfg.num_products
    categories, brands, prices = [], [], []
    for i in range(n):
        category, brand_pool, median_price, sigma = PRODUCT_CATALOG[i % len(PRODUCT_CATALOG)]
        categories.append(category)
        brands.append(brand_pool[rng.integers(len(brand_pool))])
        prices.append(round(float(rng.lognormal(math.log(median_price), sigma)), 2))

    names = [
        f"{brands[i]} {PRODUCT_NOUNS[rng.integers(len(PRODUCT_NOUNS))]} {100 + i}"
        for i in range(n)
    ]
    return pl.DataFrame(
        {
            "product_id": [f"P-{i + 1:05d}" for i in range(n)],
            "product_name": names,
            "category": categories,
            "brand": brands,
            "unit_price": prices,
        },
        schema_overrides={"unit_price": pl.Float64},
    )


def generate_customers(cfg: GeneratorConfig, rng: np.random.Generator, days: np.ndarray) -> pl.DataFrame:
    """Customers: 5k profiles with signup dates up to 12 months before the range start."""
    n = cfg.num_customers
    first = rng.choice(FIRST_NAMES, size=n)
    last = rng.choice(LAST_NAMES, size=n)
    domains = rng.choice(EMAIL_DOMAINS, size=n)
    idx = np.arange(n)
    # unique emails: first.last.<id>@domain (id guarantees uniqueness)
    emails = [f"{fn}.{ln.lower()}.{i + 1}@{dom}" for fn, ln, i, dom in zip(first, last, idx, domains)]

    country_idx = rng.choice(len(COUNTRIES), size=n, p=_country_probs())
    countries = [COUNTRIES[i][0] for i in country_idx]
    cities = [COUNTRIES[i][1][rng.integers(len(COUNTRIES[i][1]))] for i in country_idx]

    # Signups: some before the observation window (up to 12 months earlier)
    start = date.fromisoformat(cfg.date_start)
    earliest_ord = (start - timedelta(days=365)).toordinal()
    signup_ord = rng.integers(earliest_ord, start.toordinal() + 1, size=n)
    signups = [date.fromordinal(int(o)) for o in signup_ord]

    return pl.DataFrame(
        {
            "customer_id": [f"C-{i + 1:06d}" for i in range(n)],
            "first_name": [str(x) for x in first],
            "last_name": [str(x) for x in last],
            "email": emails,
            "signup_date": signups,
            "country": countries,
            "city": cities,
            "marketing_channel": [str(x) for x in rng.choice(MARKETING_CHANNELS, size=n)],
            "age": [int(x) for x in rng.integers(18, 78, size=n)],
        },
        schema_overrides={"signup_date": pl.Date, "age": pl.Int32},
    )


def _country_probs() -> np.ndarray:
    return np.array([0.50, 0.12, 0.12, 0.08, 0.08, 0.10])


def generate_orders(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    days: np.ndarray,
    customer_ids: np.ndarray,
) -> tuple[pl.DataFrame, np.ndarray]:
    """Orders: 50k orders, Zipf-skewed across customers, seasonal timing.

    Returns (orders_df, order_day_indices) for downstream date arithmetic.
    """
    n = cfg.num_orders
    n_cust = len(customer_ids)
    # Zipf-like popularity: a few customers place many orders.
    p = 1.0 / np.arange(1, n_cust + 1, dtype=np.float64) ** 0.7
    p /= p.sum()
    cust_idx = rng.choice(n_cust, size=n, p=p)

    day_idx = rng.choice(len(days), size=n, p=_seasonal_day_weights(days))
    order_date = days[day_idx]

    status = rng.choice(ORDER_STATUSES, size=n, p=ORDER_STATUS_P)
    channel = rng.choice(CHANNELS, size=n, p=CHANNEL_P)

    df = pl.DataFrame(
        {
            "order_id": [f"ORD-{i + 1:07d}" for i in range(n)],
            "customer_id": customer_ids[cust_idx],
            "order_date": _dates_to_list(order_date),
            "order_status": [str(s) for s in status],
            "channel": [str(c) for c in channel],
        },
        schema_overrides={"order_date": pl.Date},
    )
    return df, day_idx


def generate_order_items(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    orders: pl.DataFrame,
    products: pl.DataFrame,
) -> pl.DataFrame:
    """Order items: 1-5 lines per order (avg ~2), price snapshot +-2% jitter."""
    order_ids = orders["order_id"].to_numpy()
    product_ids = products["product_id"].to_numpy()
    base_prices = products["unit_price"].to_numpy()

    items_per_order = rng.choice([1, 2, 3, 4, 5], size=len(orders), p=[0.45, 0.30, 0.15, 0.07, 0.03])
    total_items = int(items_per_order.sum())

    # Broadcast order ids to per-item rows (deterministic order).
    oids = np.repeat(order_ids, items_per_order)
    prod_idx = rng.integers(0, len(product_ids), size=total_items)
    qty = rng.integers(1, 6, size=total_items)  # 1..5
    unit_price = np.round(base_prices[prod_idx] * (1.0 + rng.uniform(-0.02, 0.02, total_items)), 2)
    line_total = np.round(qty * unit_price, 2)

    return pl.DataFrame(
        {
            "order_item_id": [f"OI-{i + 1:08d}" for i in range(total_items)],
            "order_id": [str(x) for x in oids],
            "product_id": [str(x) for x in product_ids[prod_idx]],
            "quantity": [int(x) for x in qty],
            "unit_price": unit_price,
            "line_total": line_total,
        },
        schema_overrides={"quantity": pl.Int32, "unit_price": pl.Float64, "line_total": pl.Float64},
    )


def attach_order_totals(orders: pl.DataFrame, items: pl.DataFrame) -> pl.DataFrame:
    """Add total_amount (sum of line totals) to each order."""
    totals = (
        items.group_by("order_id", maintain_order=True)
        .agg(pl.col("line_total").sum().alias("total_amount"))
    )
    return orders.join(totals, on="order_id", how="left").with_columns(
        pl.col("total_amount").round(2).fill_null(0.0)
    )


def generate_payments(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    orders: pl.DataFrame,
) -> pl.DataFrame:
    """One payment per order; cancelled orders have failed payments."""
    status = orders["order_status"].to_numpy()
    cancelled = status == "cancelled"

    method = rng.choice(PAYMENT_METHODS, size=len(orders), p=PAYMENT_METHOD_P)
    # completed/returned: mostly succeeded, small pending tail; cancelled: failed
    ok_p = np.where(cancelled, 0.0, 0.97)
    succeeded = (rng.random(len(orders)) < ok_p) & ~cancelled
    pending = (~succeeded) & ~cancelled
    failed = cancelled

    df = orders.select(["order_id", "order_date", "total_amount"]).clone()
    return pl.DataFrame(
        {
            "payment_id": [f"PAY-{i + 1:07d}" for i in range(len(orders))],
            "order_id": df["order_id"].to_list(),
            "payment_method": [str(x) for x in method],
            "payment_status": [
                ("failed" if f else ("pending" if p else "succeeded"))
                for f, p in zip(failed, pending)
            ],
            "payment_date": df["order_date"].to_list(),
            "amount": df["total_amount"].to_list(),
        },
        schema_overrides={"payment_date": pl.Date, "amount": pl.Float64},
    )


def generate_refunds(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    orders: pl.DataFrame,
) -> pl.DataFrame:
    """Refunds: only for returned orders; 30% partial; lag of 3-45 days."""
    oids = orders["order_id"].to_numpy()
    custs = orders["customer_id"].to_numpy()
    dates = orders["order_date"].to_numpy()  # datetime64[D]
    totals = orders["total_amount"].to_numpy()
    status = orders["order_status"].to_numpy()

    mask = status == "returned"
    n = int(mask.sum())
    if n == 0:
        return pl.DataFrame(
            schema={
                "refund_id": pl.Utf8, "order_id": pl.Utf8, "customer_id": pl.Utf8,
                "refund_date": pl.Date, "refund_amount": pl.Float64, "refund_reason": pl.Utf8,
            }
        )

    refund_date = dates[mask] + rng.integers(3, 46, size=n).astype("timedelta64[D]")
    full = rng.random(n) < 0.7
    fraction = np.where(full, 1.0, rng.uniform(0.3, 0.8, size=n))
    amount = np.round(totals[mask] * fraction, 2)

    return pl.DataFrame(
        {
            "refund_id": [f"REF-{i + 1:06d}" for i in range(n)],
            "order_id": [str(x) for x in oids[mask]],
            "customer_id": [str(x) for x in custs[mask]],
            "refund_date": _dates_to_list(refund_date),
            "refund_amount": amount,
            "refund_reason": [str(x) for x in rng.choice(REFUND_REASONS, size=n, p=REFUND_REASON_P)],
        },
        schema_overrides={"refund_date": pl.Date, "refund_amount": pl.Float64},
    )


def generate_web_events(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    days: np.ndarray,
    customer_ids: np.ndarray,
) -> pl.DataFrame:
    """Web events: ~30% anonymous, weighted event types/devices."""
    n = cfg.num_web_events
    day_idx = rng.choice(len(days), size=n, p=_seasonal_day_weights(days))

    known = rng.random(n) < 0.70
    cust_idx = rng.integers(0, len(customer_ids), size=n)
    cust_arr = np.where(known, customer_ids[cust_idx], None)

    return pl.DataFrame(
        {
            "event_id": [f"EVT-{i + 1:08d}" for i in range(n)],
            "customer_id": [None if c is None else str(c) for c in cust_arr],
            "event_type": [str(x) for x in rng.choice(EVENT_TYPES, size=n, p=EVENT_TYPE_P)],
            "category": [str(x) for x in rng.choice([c[0] for c in PRODUCT_CATALOG], size=n)],
            "device": [str(x) for x in rng.choice(DEVICES, size=n, p=DEVICE_P)],
            "event_date": _dates_to_list(days[day_idx]),
        },
        schema_overrides={"event_date": pl.Date},
    )


def generate_support_tickets(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    days: np.ndarray,
    customer_ids: np.ndarray,
    orders: pl.DataFrame,
) -> pl.DataFrame:
    """Support tickets: ~60% linked to one of the customer's orders."""
    n = cfg.num_support_tickets
    day_idx = rng.choice(len(days), size=n)

    cust_idx = rng.choice(len(customer_ids), size=n)
    cust_arr = customer_ids[cust_idx]

    # customer -> their order ids (for realistic links)
    orders_by_customer: dict[str, list[str]] = {}
    for c, o in zip(orders["customer_id"].to_list(), orders["order_id"].to_list()):
        orders_by_customer.setdefault(c, []).append(o)

    order_refs: list[str | None] = []
    for i in range(n):
        if rng.random() < 0.6:
            mine = orders_by_customer.get(cust_arr[i])
            order_refs.append(mine[rng.integers(len(mine))] if mine else None)
        else:
            order_refs.append(None)

    return pl.DataFrame(
        {
            "ticket_id": [f"T-{i + 1:05d}" for i in range(n)],
            "customer_id": [str(x) for x in cust_arr],
            "order_id": order_refs,
            "issue_type": [str(x) for x in rng.choice(ISSUE_TYPES, size=n, p=ISSUE_TYPE_P)],
            "priority": [str(x) for x in rng.choice(TICKET_PRIORITIES, size=n, p=TICKET_PRIORITY_P)],
            "ticket_date": _dates_to_list(days[day_idx]),
            "status": [str(x) for x in rng.choice(TICKET_STATUSES, size=n, p=TICKET_STATUS_P)],
        },
        schema_overrides={"ticket_date": pl.Date},
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def generate_dataset(cfg: GeneratorConfig) -> dict[str, pl.DataFrame]:
    """Generate all tables, wired into a referentially consistent dataset."""
    start = date.fromisoformat(cfg.date_start)
    end = date.fromisoformat(cfg.date_end)
    if start > end:
        raise ValueError(f"date_start {cfg.date_start} is after date_end {cfg.date_end}")
    days = _dates_between(start, end)

    # One child RNG per entity => independent, reproducible streams.
    r_products, r_customers, r_orders, r_items, r_payments, r_refunds, r_events, r_tickets = _child_rngs(
        cfg.seed, 8
    )

    products = generate_products(cfg, r_products)
    customers = generate_customers(cfg, r_customers, days)
    customer_ids = customers["customer_id"].to_numpy()

    orders, _order_day_idx = generate_orders(cfg, r_orders, days, customer_ids)
    items = generate_order_items(cfg, r_items, orders, products)
    orders = attach_order_totals(orders, items)

    payments = generate_payments(cfg, r_payments, orders)
    refunds = generate_refunds(cfg, r_refunds, orders)
    events = generate_web_events(cfg, r_events, days, customer_ids)
    tickets = generate_support_tickets(cfg, r_tickets, days, customer_ids, orders)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": items,
        "payments": payments,
        "refunds": refunds,
        "web_events": events,
        "support_tickets": tickets,
    }


def write_dataset(frames: dict[str, pl.DataFrame], cfg: GeneratorConfig, out_dir: Path) -> Path:
    """Write Parquet files + manifest.json; returns the manifest path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    tables: dict[str, dict] = {}
    for name, df in frames.items():
        path = out_dir / f"{name}.parquet"
        df.write_parquet(path)
        tables[name] = {
            "path": f"{name}.parquet",
            "rows": len(df),
            "columns": _dtypes_of(df),
            "sha256": _sha256_of_frame(df),
        }

    manifest = {
        "generator": "data_generator/generate_synthetic.py",
        "seed": cfg.seed,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "date_range": [cfg.date_start, cfg.date_end],
        "tables": tables,
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=GeneratorConfig().seed)
    parser.add_argument("--date-start", default=GeneratorConfig().date_start)
    parser.add_argument("--date-end", default=GeneratorConfig().date_end)
    parser.add_argument("--num-customers", type=int, default=GeneratorConfig().num_customers)
    parser.add_argument("--num-orders", type=int, default=GeneratorConfig().num_orders)
    parser.add_argument("--num-products", type=int, default=GeneratorConfig().num_products)
    parser.add_argument("--num-web-events", type=int, default=GeneratorConfig().num_web_events)
    parser.add_argument("--num-support-tickets", type=int, default=GeneratorConfig().num_support_tickets)
    parser.add_argument(
        "--out-dir",
        default=str(REPO_ROOT / "data" / "synthetic"),
        help="Output directory (default: <repo>/data/synthetic)",
    )
    args = parser.parse_args(argv)

    cfg = GeneratorConfig(
        seed=args.seed,
        date_start=args.date_start,
        date_end=args.date_end,
        num_customers=args.num_customers,
        num_orders=args.num_orders,
        num_products=args.num_products,
        num_web_events=args.num_web_events,
        num_support_tickets=args.num_support_tickets,
    )
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir

    print(f"Generating synthetic e-commerce data (seed={cfg.seed})...")
    frames = generate_dataset(cfg)
    manifest_path = write_dataset(frames, cfg, out_dir)

    total = sum(len(df) for df in frames.values())
    print(f"{'table':<18}{'rows':>10}")
    for name, df in frames.items():
        print(f"{name:<18}{len(df):>10,}")
    print(f"{'TOTAL':<18}{total:>10,}")
    print(f"Output:   {out_dir}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
