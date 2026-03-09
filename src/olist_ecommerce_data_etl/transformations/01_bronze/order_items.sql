CREATE OR REFRESH STREAMING TABLE workspace.default.bronze_olist_order_items
AS   
  SELECT
    *
  FROM
    STREAM read_files(
      "/Volumes/workspace/default/olist_ecommerce/order_items",
      format => "csv",
      delimiter => ",",
      header => "true"
    )
;