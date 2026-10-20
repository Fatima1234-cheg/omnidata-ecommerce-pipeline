from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("OmniData - Gold RFM").getOrCreate()

    # Lecture des tables Silver
    orders = spark.read.parquet("/app/data/silver/orders_spark.parquet")
    payments = spark.read.parquet("/app/data/silver/payments_spark.parquet")
    customers = spark.read.parquet("/app/data/silver/customers_spark.parquet")

    # 1. Jointure pour récupérer l'identifiant unique du client (customer_unique_id)
    orders_customers = orders.join(
        customers.select("customer_id", "customer_unique_id"), 
        on="customer_id", 
        how="left"
    )

    # 2. On ne garde que les commandes livrées
    valid_orders = orders_customers.filter(F.col("order_status") == "delivered")

    # 3. On joint avec les paiements et on agrège par client unique
    customer_orders = valid_orders.join(payments, on="order_id", how="inner") \
        .groupBy("customer_unique_id").agg(
            F.max("order_purchase_timestamp").alias("last_purchase_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("payment_value").alias("monetary")
        )

    # 4. Calcul de la Récence (en jours)
    max_date = valid_orders.agg(F.max("order_purchase_timestamp")).collect()[0][0]
    
    rfm = customer_orders.withColumn(
        "recency_days", 
        F.datediff(F.lit(max_date), F.col("last_purchase_date"))
    ).drop("last_purchase_date")

    print("\n=== APERÇU RFM ===")
    rfm.show(5)
    print(f"Nombre total de clients uniques : {rfm.count()}")

    print("Sauvegarde en Parquet...")
    rfm.write.mode("overwrite").parquet("/app/data/gold/gold_customer_rfm.parquet")
    spark.stop()

if __name__ == "__main__":
    main()