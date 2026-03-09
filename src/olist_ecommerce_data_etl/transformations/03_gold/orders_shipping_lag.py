from pyspark import pipelines as dp
from pyspark.sql.functions import col, datediff, when


@dp.materialized_view
def gold_olist_orders_shipping_lag():
    return (
        spark.read
            .table("workspace.default.silver_olist_orders_cleaned")
            .filter(col("order_delivered_carrier_date").isNotNull())
            .select(
                datediff(col("order_delivered_carrier_date"), col("order_approved_at")).alias("shipping_lag")
            )
    )


@dp.materialized_view
def gold_olist_orders_shipping_lag_category():
    return (
        spark.read
            .table("workspace.default.gold_olist_orders_shipping_lag")
            .withColumn(
                "shipping_lag_category", 
                when(col("shipping_lag") <= 2, 'On-time').otherwise('Delay')
            )
            .groupBy("shipping_lag_category")
            .count()
    )   