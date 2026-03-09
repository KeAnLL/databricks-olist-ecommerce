CREATE OR REFRESH STREAMING TABLE workspace.default.bronze_olist_order_reviews
AS
  SELECT
    *
  FROM
    STREAM read_files(
      "/Volumes/workspace/default/olist_ecommerce/order_reviews",
      format => "csv",
      delimiter => ",",
      header => "true"
    )
;