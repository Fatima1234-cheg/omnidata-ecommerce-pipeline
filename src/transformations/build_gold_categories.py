from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("OmniData - Gold Categories").getOrCreate()

    items = spark.read.parquet("/app/data/silver/order_items_spark.parquet")
    products = spark.read.parquet("/app/data/silver/products_spark.parquet")

    # Jointure
    sales = items.join(products, on="product_id", how="left")

    # Agrégation par catégorie
    gold_categories = sales.groupBy("product_category_name_english").agg(
        F.sum("price").alias("total_revenue"),
        F.count("order_id").alias("total_items_sold")
    ).orderBy(F.desc("total_revenue"))

    print("Sauvegarde de la table Gold Categories...")
    gold_categories.write.mode("overwrite").parquet("/app/data/gold/gold_product_categories.parquet")
    spark.stop()

if __name__ == "__main__":
    main()