CREATE OR REPLACE MATERIALIZED VIEW workspace.default.gold_olist_order_payments
AS
  SELECT
    order_id, 
    collect_set(payment_type) payment_types,
    SUM(payment_installments - 1) + 1 payment_installments,
    SUM(payment_value) payment_value
  FROM
    workspace.default.silver_olist_orders_cleaned_with_payments
  GROUP BY
    order_id
;