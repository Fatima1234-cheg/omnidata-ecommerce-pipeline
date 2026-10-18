
from pathlib import Path

import pandas as pd


# Racine du projet
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]


def inspect_csv(filename: str) -> None:
    file_path = RAW_DIR / filename

    print("\n" + "=" * 65)
    print(f"FICHIER : {filename}")
    print("=" * 65)

    if not file_path.exists():
        print("Fichier introuvable.")
        return

    # Lire uniquement un échantillon
    df = pd.read_csv(file_path, nrows=1000)

    print(f"Colonnes : {len(df.columns)}")
    print("\nNoms des colonnes :")
    print(df.columns.tolist())

    print("\nTypes des données :")
    print(df.dtypes)

    print("\nValeurs manquantes dans l'échantillon :")
    print(df.isnull().sum())

    print("\nAperçu :")
    print(df.head(3))


def main() -> None:
    for filename in FILES:
        inspect_csv(filename)


if __name__ == "__main__":
    main()


#Ce script va vérifier les fichiers présents, 
# lire un échantillon de chaque CSV et afficher les colonnes,
# les types et les valeurs manquantes, sans charger tous les fichiers en mémoire