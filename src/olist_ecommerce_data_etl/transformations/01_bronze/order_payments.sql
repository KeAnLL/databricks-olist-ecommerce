CREATE OR REFRESH STREAMING TABLE workspace.default.bronze_olist_order_payments
AS
  SELECT
    *
  FROM
    STREAM read_files(
      "/Volumes/workspace/default/olist_ecommerce/order_payments",
      format => "csv",
      delimiter => ",",
      header => "true"
    )
;