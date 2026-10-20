from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("OmniData - Clean Customers").getOrCreate()

    RAW_FILE = "/app/data/raw/olist_customers_dataset.csv"
    SILVER_DIR = "/app/data/silver/customers_spark.parquet"

    print("Chargement des clients...")
    customers = spark.read.csv(RAW_FILE, header=True, inferSchema=True)

    # On s'assure que le code postal est bien un texte (pour ne pas perdre les zéros au début)
    customers = customers.withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("string"))

    print("\n=== CONTROLES SILVER (CUSTOMERS) ===")
    print("Nombre de clients :", customers.count())
    
    # Aperçu des États (très utile pour Power BI plus tard)
    customers.groupBy("customer_state").count().orderBy(F.desc("count")).show(5)

    print("Sauvegarde en Parquet...")
    customers.write.mode("overwrite").parquet(SILVER_DIR)
    print("Fichier créé :", SILVER_DIR)
    spark.stop()

if __name__ == "__main__":
    main()