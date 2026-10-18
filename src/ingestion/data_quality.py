
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Clés qui devraient identifier une ligne de façon unique.
UNIQUE_KEYS = {
    "olist_orders_dataset.csv": ["order_id"],
    "olist_customers_dataset.csv": ["customer_id"],
    "olist_order_items_dataset.csv": ["order_id", "order_item_id"],
    "olist_order_payments_dataset.csv": [
        "order_id", "payment_sequential"
    ],
    "olist_products_dataset.csv": ["product_id"],
    "olist_sellers_dataset.csv": ["seller_id"],
    "product_category_name_translation.csv": ["product_category_name"],
}

def load_csv(filename, columns=None):
    """Charge un CSV et signale clairement les fichiers absents."""
    path = RAW_DIR / filename

    if not path.exists():
        print(f"FICHIER INTROUVABLE : {path}")
        return None

    return pd.read_csv(path, usecols=columns, low_memory=False)


def check_duplicates():
    print("\n" + "=" * 60)
    print("1. CONTRÔLE DES DOUBLONS")
    print("=" * 60)

    for filename, keys in UNIQUE_KEYS.items():
        df = load_csv(filename)

        if df is None:
            continue

        duplicate_count = int(df.duplicated(subset=keys).sum())

        print(f"\n{filename}")
        print(f"  Nombre de lignes : {len(df):,}")
        print(f"  Doublons sur {keys} : {duplicate_count:,}")


def check_missing_values():
    print("\n" + "=" * 60)
    print("2. VALEURS MANQUANTES")
    print("=" * 60)

    files = [
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
    ]

    for filename in files:
        df = load_csv(filename)

        if df is None:
            continue

        missing = df.isna().sum()
        missing = missing[missing > 0]

        print(f"\n{filename}")

        if missing.empty:
            print("  Aucune valeur manquante.")
        else:
            for column, count in missing.items():
                percentage = count / len(df) * 100
                print(
                    f"  {column}: {count:,} "
                    f"({percentage:.2f} %)"
                )


def check_order_dates():
    print("\n" + "=" * 60)
    print("3. COHERENCE DES DATES DE COMMANDE")
    print("=" * 60)

    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    df = load_csv(
        "olist_orders_dataset.csv",
        columns=["order_id"] + date_columns,
    )

    if df is None:
        return

    for column in date_columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")

    # Une livraison effective avant l'achat est incohérente.
    invalid_delivery = (
        df["order_delivered_customer_date"].notna()
        & (
            df["order_delivered_customer_date"]
            < df["order_purchase_timestamp"]
        )
    )

    # Une livraison avant la remise au transporteur est suspecte.
    invalid_carrier = (
        df["order_delivered_customer_date"].notna()
        & df["order_delivered_carrier_date"].notna()
        & (
            df["order_delivered_customer_date"]
            < df["order_delivered_carrier_date"]
        )
    )

    # Les dates manquantes ne sont pas automatiquement des erreurs :
    # une commande peut être annulée ou encore en cours.
    print(
        "Livraison avant achat :",
        int(invalid_delivery.sum()),
    )
    print(
        "Livraison avant transporteur :",
        int(invalid_carrier.sum()),
    )
    print(
        "Date de livraison client manquante :",
        int(df["order_delivered_customer_date"].isna().sum()),
    )


def check_foreign_keys():
    print("\n" + "=" * 60)
    print("4. CONTROLE DES RELATIONS ENTRE TABLES")
    print("=" * 60)

    orders = load_csv(
        "olist_orders_dataset.csv", ["order_id", "customer_id"]
    )
    customers = load_csv(
        "olist_customers_dataset.csv", ["customer_id"]
    )
    items = load_csv(
        "olist_order_items_dataset.csv",
        ["order_id", "product_id", "seller_id"],
    )
    products = load_csv(
        "olist_products_dataset.csv", ["product_id"]
    )
    sellers = load_csv(
        "olist_sellers_dataset.csv", ["seller_id"]
    )
    payments = load_csv(
        "olist_order_payments_dataset.csv", ["order_id"]
    )

    if any(
        df is None
        for df in [
            orders, customers, items, products, sellers, payments
        ]
    ):
        print("Contrôle incomplet : un ou plusieurs fichiers manquent.")
        return

    order_ids = set(orders["order_id"].dropna())
    customer_ids = set(customers["customer_id"].dropna())
    product_ids = set(products["product_id"].dropna())
    seller_ids = set(sellers["seller_id"].dropna())

    checks = {
        "Commandes avec client absent":
            (~orders["customer_id"].isin(customer_ids)).sum(),
        "Articles avec commande absente":
            (~items["order_id"].isin(order_ids)).sum(),
        "Articles avec produit absent":
            (~items["product_id"].isin(product_ids)).sum(),
        "Articles avec vendeur absent":
            (~items["seller_id"].isin(seller_ids)).sum(),
        "Paiements avec commande absente":
            (~payments["order_id"].isin(order_ids)).sum(),
    }

    for label, count in checks.items():
        print(f"{label} : {int(count):,}")


def main():
    check_duplicates()
    check_missing_values()
    check_order_dates()
    check_foreign_keys()

    print("\nContrôle qualité terminé.")


if __name__ == "__main__":
    main()
