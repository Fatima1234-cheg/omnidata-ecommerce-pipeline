from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("OmniData - Clean Payments").getOrCreate()

    RAW_FILE = "/app/data/raw/olist_order_payments_dataset.csv"
    SILVER_DIR = "/app/data/silver/payments_spark.parquet"

    print("Chargement des paiements...")
    payments = spark.read.csv(RAW_FILE, header=True, inferSchema=True)

    # Conversion des types
    payments = payments.withColumn("payment_value", F.col("payment_value").cast("double")) \
                       .withColumn("payment_installments", F.col("payment_installments").cast("integer"))

    print("\n=== CONTROLES SILVER (PAYMENTS) ===")
    print("Nombre de paiements :", payments.count())
    
    # Aperçu des types de paiement
    payments.groupBy("payment_type").count().orderBy(F.desc("count")).show()

    print("Sauvegarde en Parquet...")
    payments.write.mode("overwrite").parquet(SILVER_DIR)
    print("Fichier créé :", SILVER_DIR)
    spark.stop()

if __name__ == "__main__":
    main()