from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("OmniData - Gold Sales").getOrCreate()

    # Chemins des fichiers Silver
    SILVER_ORDERS = "/app/data/silver/orders_spark.parquet"
    SILVER_ITEMS = "/app/data/silver/order_items_spark.parquet"
    SILVER_PAYMENTS = "/app/data/silver/payments_spark.parquet"
    
    # Chemin de sortie Gold
    GOLD_DIR = "/app/data/gold/gold_sales_orders.parquet"

    print("Chargement des tables Silver...")
    orders = spark.read.parquet(SILVER_ORDERS)
    items = spark.read.parquet(SILVER_ITEMS)
    payments = spark.read.parquet(SILVER_PAYMENTS)

    # 1. On agrège les articles par commande (Somme des prix + frais de port)
    items_agg = items.groupBy("order_id").agg(
        F.sum("price").alias("total_items_price"),
        F.sum("freight_value").alias("total_freight")
    )

    # 2. On agrège les paiements par commande (Somme de ce qui a été payé)
    payments_agg = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("total_paid")
    )

    # 3. On joint tout ça à la table des commandes
    # On filtre pour ne garder que les commandes livrées ou expédiées (pour le vrai CA)
    valid_orders = orders.filter(F.col("order_status").isin("delivered", "shipped", "invoiced"))

    gold_df = valid_orders.join(items_agg, on="order_id", how="inner") \
                         .join(payments_agg, on="order_id", how="left")

    # 4. On ajoute des colonnes de temps (Année et Mois) pour les graphiques Power BI
    gold_df = gold_df.withColumn("order_year", F.year("order_purchase_timestamp")) \
                     .withColumn("order_month", F.month("order_purchase_timestamp"))

    print("\n=== APERÇU DE LA COUCHE GOLD ===")
    gold_df.select("order_id", "order_year", "order_month", "total_items_price", "total_paid").show(5)

    print(f"Nombre de commandes valides : {gold_df.count()}")

    print("Sauvegarde en Parquet dans le dossier Gold...")
    gold_df.write.mode("overwrite").parquet(GOLD_DIR)
    print("Fichier Gold créé :", GOLD_DIR)

    spark.stop()

if __name__ == "__main__":
    main()