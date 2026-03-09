CREATE OR REFRESH STREAMING TABLE workspace.default.silver_olist_orders_cleaned_with_payments
AS
  SELECT
    orders.order_id,
    customer_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
  FROM  
    STREAM(workspace.default.silver_olist_orders_cleaned) orders
      JOIN
    STREAM(workspace.default.bronze_olist_order_payments) payments
        ON
          orders.order_id = payments.order_id
;