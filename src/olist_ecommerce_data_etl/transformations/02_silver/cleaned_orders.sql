CREATE OR REFRESH STREAMING TABLE workspace.default.silver_olist_orders_cleaned
AS
  SELECT
    *
  FROM
    STREAM(workspace.default.bronze_olist_orders)
  WHERE
    order_status IS NOT NULL
;