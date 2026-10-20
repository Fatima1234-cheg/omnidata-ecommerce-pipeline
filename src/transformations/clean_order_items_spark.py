from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("OmniData - Clean Order Items") \
        .getOrCreate()

    # Chemins pour Docker
    RAW_FILE = "/app/data/raw/olist_order_items_dataset.csv"
    SILVER_DIR = "/app/data/silver/order_items_spark.parquet"

    print("Chargement des articles...")
    items = spark.read.csv(RAW_FILE, header=True, inferSchema=True)

    # 1. Conversion des types (prix et frais en nombres décimaux)
    items = items.withColumn("price", F.col("price").cast("double")) \
                 .withColumn("freight_value", F.col("freight_value").cast("double"))
    
    # 2. Conversion de la date d'expédition limite
    items = items.withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))

    print("\n=== CONTROLES SILVER (ORDER ITEMS) ===")
    print("Nombre d'articles :", items.count())
    
    # Vérifier qu'il n'y a pas de prix négatifs ou nuls
    invalid_prices = items.filter(F.col("price") <= 0).count()
    print("Articles avec un prix <= 0 :", invalid_prices)

    print("Sauvegarde en Parquet...")
    items.write.mode("overwrite").parquet(SILVER_DIR)
    print("Fichier créé :", SILVER_DIR)

    spark.stop()

if __name__ == "__main__":
    main()