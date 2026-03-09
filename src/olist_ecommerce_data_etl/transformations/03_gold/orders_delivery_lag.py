from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, datediff


@dp.materialized_view
def gold_olist_orders_delivery_lag():
    return (
        spark.read
            .table("workspace.default.silver_olist_orders_cleaned")
            .filter(col("order_status") == 'delivered')
            .filter(col("order_delivered_customer_date").isNotNull())
            .select(
                datediff(col("order_estimated_delivery_date"), col("order_delivered_customer_date")).alias("delivery_lag")
            )
    )


@dp.materialized_view
def gold_olist_orders_delivery_lag_category():
    return (
        spark.read
            .table("workspace.default.gold_olist_orders_delivery_lag")
            .withColumn(
                "delivery_lag_category",
                when(col("delivery_lag") < 0, 'Later than estimated').
                when(col("delivery_lag") == 0, 'On time').
                when(col("delivery_lag") > 0, 'Earlier than estimated')
            )
            .groupBy('delivery_lag_category').
            count()
    )