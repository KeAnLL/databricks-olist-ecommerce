CREATE OR REFRESH STREAMING TABLE workspace.default.bronze_olist_orders
AS
  SELECT
    *
  FROM
    STREAM read_files(
      "/Volumes/workspace/default/olist_ecommerce/orders",
      format => "csv",
      delimiter => ",",
      header => "true"
    )
;