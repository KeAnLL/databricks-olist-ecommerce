CREATE OR REFRESH STREAMING TABLE workspace.default.silver_olist_orders_cleaned_with_reviews
AS
  SELECT
    order_id,
    review_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
  FROM
    STREAM(workspace.default.silver_olist_orders_cleaned) orders
      JOIN
    STREAM(workspace.default.bronze_olist_order_reviews) reviews
        USING
          (order_id)
;