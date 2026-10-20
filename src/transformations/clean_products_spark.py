from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # Démarrage de Spark
    spark = SparkSession.builder.appName("OmniData - Clean Products").getOrCreate()

    # Chemins pour Docker
    RAW_PRODUCTS = "/app/data/raw/olist_products_dataset.csv"
    RAW_TRANSLATION = "/app/data/raw/product_category_name_translation.csv"
    SILVER_DIR = "/app/data/silver/products_spark.parquet"

    print("Chargement des produits et du fichier de traduction...")
    products = spark.read.csv(RAW_PRODUCTS, header=True, inferSchema=True)
    translation = spark.read.csv(RAW_TRANSLATION, header=True, inferSchema=True)

    # 1. Jointure (LEFT JOIN) pour ajouter la colonne en anglais
    products_clean = products.join(
        translation, 
        on="product_category_name", 
        how="left"
    )

    # 2. Remplacer les catégories vides ou sans traduction par "Unknown"
    products_clean = products_clean.fillna({"product_category_name_english": "Unknown"})

    print("\n=== CONTROLES SILVER (PRODUCTS) ===")
    print("Nombre de produits :", products_clean.count())
    
    # Aperçu du résultat (ID, Catégorie en portugais, Catégorie en anglais)
    products_clean.select("product_id", "product_category_name", "product_category_name_english").show(5)

    print("Sauvegarde en Parquet...")
    products_clean.write.mode("overwrite").parquet(SILVER_DIR)
    print("Fichier créé :", SILVER_DIR)
    
    spark.stop()

if __name__ == "__main__":
    main()