CREATE OR REFRESH STREAMING TABLE workspace.default.silver_olist_orders_cleaned_with_order_items
AS
  SELECT 
    orders.order_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_item_id,
    product_id,
    price,
    shipping_limit_date,
    freight_value
  FROM
    STREAM(workspace.default.silver_olist_orders_cleaned) orders
      JOIN
    STREAM(workspace.default.bronze_olist_order_items) order_items
        WHERE 
          orders.order_id = order_items.order_id
;