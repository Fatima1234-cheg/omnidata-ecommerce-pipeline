
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

orders_path = RAW_DIR / "olist_orders_dataset.csv"

df = pd.read_csv(orders_path)

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

for column in date_columns:
    df[column] = pd.to_datetime(df[column], errors="coerce")

print("\n1. STATUTS DES COMMANDES")
print(df["order_status"].value_counts(dropna=False))

print("\n2. COMMANDES SANS DATE DE LIVRAISON")
not_delivered = df[
    df["order_delivered_customer_date"].isna()
]

print(
    not_delivered["order_status"]
    .value_counts(dropna=False)
)

print("\n3. LES 23 DATES SUSPECTES")
invalid = df[
    df["order_delivered_customer_date"].notna()
    & df["order_delivered_carrier_date"].notna()
    & (
        df["order_delivered_customer_date"]
        < df["order_delivered_carrier_date"]
    )
]

columns_to_display = [
    "order_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
]

print(invalid[columns_to_display].to_string(index=False))

print("\nNombre de cas suspects :", len(invalid))
