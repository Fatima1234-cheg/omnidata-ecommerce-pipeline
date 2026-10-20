from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # 1. Démarrage de Spark (en mode local dans le conteneur)
    spark = SparkSession.builder \
        .appName("OmniData - Clean Orders (Docker)") \
        .master("local[*]") \
        .getOrCreate()

    # 2. Définition des chemins (qui feront référence au volume monté)
    # Le conteneur verra tes données dans /app/data
    RAW_FILE = "/app/data/raw/olist_orders_dataset.csv"
    SILVER_DIR = "/app/data/silver/orders_spark.parquet"

    # 3. Lecture du CSV
    print("Chargement des données brutes...")
    orders = spark.read.csv(RAW_FILE, header=True, inferSchema=True)

    # 4. Conversion des dates
    date_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col_name in date_cols:
        orders = orders.withColumn(col_name, F.to_timestamp(col_name))

    # 5. Création du "post-it rouge" (chronologie valide ou non)
    orders = orders.withColumn(
        "is_delivery_chronology_valid",
        F.when(
            (F.col("order_delivered_customer_date").isNotNull()) & 
            (F.col("order_delivered_carrier_date").isNotNull()) & 
            (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date")), 
            F.lit(False)
        ).otherwise(F.lit(True))
    )

    # 6. Calcul de la durée de livraison
    purchase = F.col("order_purchase_timestamp")
    delivered = F.col("order_delivered_customer_date")
    
    valid_duration = (
        (purchase.isNotNull()) & 
        (delivered.isNotNull()) & 
        (F.col("is_delivery_chronology_valid") == True) & 
        (delivered >= purchase)
    )

    orders = orders.withColumn(
        "delivery_duration_days",
        F.when(
            valid_duration,
            (delivered.cast("long") - purchase.cast("long")) / F.lit(86400.0)
        ).otherwise(F.lit(None).cast("double"))
    )

    # 7. Affichage des contrôles
    print("\n=== CONTROLES SILVER ===")
    print("Nombre total de lignes :", orders.count())
    orders.groupBy("is_delivery_chronology_valid").count().show()

    # 8. Sauvegarde en Parquet
    print("Sauvegarde dans Silver...")
    orders.write.mode("overwrite").parquet(SILVER_DIR)
    print(f"Fichier Silver créé : {SILVER_DIR}")

    # 9. Arrêt de Spark
    spark.stop()

if __name__ == "__main__":
    main()