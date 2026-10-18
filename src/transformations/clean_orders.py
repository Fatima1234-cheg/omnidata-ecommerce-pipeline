
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = RAW_DIR / "olist_orders_dataset.csv"
BRONZE_FILE = BRONZE_DIR / "orders.parquet"
SILVER_FILE = SILVER_DIR / "orders.parquet"

DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def main():
    # 1. Lecture du fichier brut
    df = pd.read_csv(INPUT_FILE)

    print("Nombre de commandes brutes :", len(df))

    # 2. Sauvegarde Bronze : données chargées sans nettoyage métier
    df.to_parquet(BRONZE_FILE, index=False)

    # 3. Conversion des colonnes de dates
    for column in DATE_COLUMNS:
        df[column] = pd.to_datetime(
            df[column], errors="coerce"
        )

    # 4. Indicateur de validité de la chronologie
    has_delivery_dates = (
        df["order_delivered_customer_date"].notna()
        & df["order_delivered_carrier_date"].notna()
    )

    df["is_delivery_date_valid"] = (
        ~has_delivery_dates
        | (
            df["order_delivered_customer_date"]
            >= df["order_delivered_carrier_date"]
        )
    )

    # 5. Calcul de la durée de livraison
    # Une durée est calculée seulement si les dates nécessaires
    # sont présentes et chronologiquement cohérentes.
    valid_duration = (
        df["order_purchase_timestamp"].notna()
        & df["order_delivered_customer_date"].notna()
        & df["is_delivery_date_valid"]
        & (
            df["order_delivered_customer_date"]
            >= df["order_purchase_timestamp"]
        )
    )

    df["delivery_duration_days"] = pd.NA

    duration = (
        df.loc[valid_duration, "order_delivered_customer_date"]
        - df.loc[valid_duration, "order_purchase_timestamp"]
    ).dt.total_seconds() / 86400

    df.loc[valid_duration, "delivery_duration_days"] = duration

    # 6. Sauvegarde Silver : données typées et enrichies
    df.to_parquet(SILVER_FILE, index=False)

    print("Nombre de commandes Silver :", len(df))
    print(
        "Chronologies invalides :",
        int((~df["is_delivery_date_valid"]).sum()),
    )
    print(
        "Commandes avec durée calculable :",
        int(df["delivery_duration_days"].notna().sum()),
    )
    print("Fichier Bronze :", BRONZE_FILE)
    print("Fichier Silver :", SILVER_FILE)


if __name__ == "__main__":
    main()
