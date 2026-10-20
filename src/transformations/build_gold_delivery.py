from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("OmniData - Gold Delivery").getOrCreate()

    # Lecture des tables Silver
    orders = spark.read.parquet("/app/data/silver/orders_spark.parquet")
    customers = spark.read.parquet("/app/data/silver/customers_spark.parquet")

    # Jointure
    delivery = orders.join(customers, on="customer_id", how="left")

    # Calcul des retards
    delivery = delivery.withColumn(
        "delay_days", 
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
    )
    delivery = delivery.withColumn(
        "is_late", 
        F.when(F.col("delay_days") > 0, 1).otherwise(0)
    )

    # Filtre sur les commandes livrées valides
    gold_delivery = delivery.filter(
        (F.col("order_status") == "delivered") & 
        (F.col("is_delivery_chronology_valid") == True)
    ).select(
        "order_id", "customer_state", "delivery_duration_days", "delay_days", "is_late"
    )

    print("Sauvegarde de la table Gold Delivery...")
    gold_delivery.write.mode("overwrite").parquet("/app/data/gold/gold_delivery_performance.parquet")
    spark.stop()

if __name__ == "__main__":
    main()