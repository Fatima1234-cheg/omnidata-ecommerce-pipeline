
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_orders_dataset.csv"
)

SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "orders_spark"


def main():
    # 1. Démarrer Spark
    spark = (
        SparkSession.builder
        .appName("OmniData-Clean-Orders")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 2. Lire le CSV
    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(RAW_FILE))
    )

    print("\n=== DONNEES BRUTES ===")
    print("Nombre de lignes :", orders.count())
    orders.show(5, truncate=False)

    # 3. Convertir les dates texte en timestamps Spark
    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for column in date_columns:
        orders = orders.withColumn(
            column,
            F.to_timestamp(
                F.col(column),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

    # 4. Créer des indicateurs de qualité
    purchase = F.col("order_purchase_timestamp")
    carrier = F.col("order_delivered_carrier_date")
    delivered = F.col("order_delivered_customer_date")

    has_delivery = delivered.isNotNull()

    chronology_is_valid = (
        purchase.isNotNull()
        & (delivered >= purchase)
        & (
            carrier.isNull()
            | (delivered >= carrier)
        )
    )

    orders = (
        orders
        .withColumn(
            "has_delivery_date",
            has_delivery,
        )
        .withColumn(
            "is_delivery_chronology_valid",
            F.when(
                ~has_delivery,
                F.lit(None).cast("boolean"),
            ).otherwise(chronology_is_valid),
        )
    )

    # 5. Calculer la durée en jours uniquement
    # si les dates disponibles sont cohérentes.
    valid_duration = (
        has_delivery
        & purchase.isNotNull()
        & chronology_is_valid
    )

    orders = orders.withColumn(
        "delivery_duration_days",
        F.when(
            valid_duration,
            (
                delivered.cast("long")
                - purchase.cast("long")
            ) / F.lit(86400.0),
        ).otherwise(F.lit(None).cast("double")),
    )

    # 6. Afficher les contrôles
    print("\n=== CONTROLES SILVER ===")
    print("Nombre de lignes :", orders.count())

    orders.groupBy("order_status").count().show()

    orders.groupBy(
        "is_delivery_chronology_valid"
    ).count().show()

    print("\n=== EXEMPLE DE RESULTAT ===")
    orders.select(
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "is_delivery_chronology_valid",
        "delivery_duration_days",
    ).show(10, truncate=False)

    # 7. Enregistrer la couche Silver en Parquet
    (
        orders.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR))
    )

    print("\nFichier Silver créé :", SILVER_DIR)

    spark.stop()


if __name__ == "__main__":
    main()
