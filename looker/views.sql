--Views utilizadas no painel do Looker Studio
--meli_case.vw_alerts_status
  CREATE OR REPLACE VIEW `meli_case.vw_discrepancies_trend_month` AS
SELECT
  DATE_TRUNC(order_date, MONTH) AS month,
  COUNT(*) AS orders,
  SUM(is_discrepant) AS discrepant_orders,
  SAFE_DIVIDE(SUM(is_discrepant), COUNT(*)) AS discrepant_rate,
  SUM(abs_diff) AS sum_abs_diff
FROM `meli_case.vw_order_recon_per_order`
GROUP BY 1;
  
--meli_case.vw_invoice_coverage_month
CREATE OR REPLACE VIEW `meli_case.vw_invoice_coverage_month` AS
WITH o AS (
  SELECT DATE_TRUNC(DATE(created_at), MONTH) AS month, order_id
  FROM `meli_case.order`
),
i AS (
  SELECT DISTINCT order_id
  FROM `meli_case.invoice`
  WHERE invoice_status = 'ISSUED'
)
SELECT
  o.month,
  COUNT(DISTINCT o.order_id) AS orders,
  COUNT(DISTINCT i.order_id) AS invoiced_orders,
  SAFE_DIVIDE(COUNT(DISTINCT i.order_id), COUNT(DISTINCT o.order_id)) AS invoice_coverage_rate
FROM o
LEFT JOIN i USING (order_id)
GROUP BY 1;

--meli_case.vw_alerts_flow_daily
CREATE OR REPLACE VIEW `meli_case.vw_alerts_flow_daily` AS
WITH days AS (
  SELECT d AS day
  FROM UNNEST(GENERATE_DATE_ARRAY(
         DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY), CURRENT_DATE())) d
),
created AS (
  SELECT DATE(creation_date) AS day, COUNT(*) AS created_cnt
  FROM `meli_case.alerts`
  GROUP BY 1
),
resolved AS (
  SELECT DATE(resolution_date) AS day, COUNT(*) AS resolved_cnt
  FROM `meli_case.alerts`
  WHERE resolution_date IS NOT NULL
  GROUP BY 1
)
SELECT
  d.day,
  COALESCE(c.created_cnt, 0)  AS created_cnt,
  COALESCE(r.resolved_cnt, 0) AS resolved_cnt,
  SUM(COALESCE(c.created_cnt, 0))  OVER (ORDER BY d.day)
  - SUM(COALESCE(r.resolved_cnt, 0)) OVER (ORDER BY d.day) AS backlog_open -- backlog = acumulo criado - acumulo resolvido
FROM days d
LEFT JOIN created c  USING (day)
LEFT JOIN resolved r USING (day);

--meli_case.vw_tax_incidents_by_category
CREATE OR REPLACE VIEW `meli_case.vw_tax_incidents_by_category` AS
WITH base AS (
  SELECT
    DATE_TRUNC(i.issue_date, MONTH) AS month,
    COALESCE(cat.category_name, 'UNK') AS category_name,
    tc.tax_type,
    ii.invoice_id
  FROM `meli_case.invoice_item` ii
  JOIN `meli_case.invoice` i
    USING (invoice_id, order_id)
  JOIN `meli_case.tax_calculation` tc
    USING (invoice_id, invoice_item_id)
  JOIN `meli_case.item` it
    USING (item_id)
  LEFT JOIN `meli_case.category` cat
    USING (category_id)
  WHERE i.invoice_status = 'ISSUED'
)
SELECT
  month,
  category_name,
  tax_type,
  COUNT(*)                         AS incidents,        -- nº de cálculos de imposto
  COUNT(DISTINCT invoice_id)       AS invoices          -- nº de faturas afetadas
FROM base
GROUP BY 1,2,3;

--meli_case.vw_discrepancies_trend_month
-- Base 1x por pedido
CREATE OR REPLACE VIEW `meli_case.vw_order_recon_per_order` AS
WITH pay AS (
  SELECT order_id, SUM(paid_amount) AS total_paid
  FROM `meli_case.payment`
  WHERE payment_status = 'APPROVED'
  GROUP BY 1
),
inv AS (
  SELECT order_id, SUM(total_invoiced) AS total_invoiced
  FROM `meli_case.invoice`
  WHERE invoice_status = 'ISSUED'
  GROUP BY 1
),
ord AS (
  SELECT order_id, DATE(created_at) AS order_date, seller_id, customer_id
  FROM `meli_case.order`
)
SELECT
  o.order_id,
  o.order_date,
  o.seller_id,
  o.customer_id,
  COALESCE(p.total_paid, 0)     AS total_paid,
  COALESCE(i.total_invoiced, 0) AS total_invoiced,
  COALESCE(p.total_paid,0) - COALESCE(i.total_invoiced,0) AS diff,
  ABS(COALESCE(p.total_paid,0) - COALESCE(i.total_invoiced,0)) AS abs_diff,
  CASE WHEN ABS(COALESCE(p.total_paid,0) - COALESCE(i.total_invoiced,0)) > 0.01 THEN 1 ELSE 0 END AS is_discrepant
FROM ord o
LEFT JOIN pay p USING (order_id)
LEFT JOIN inv i USING (order_id);

CREATE OR REPLACE VIEW `meli_case.vw_discrepancies_trend_month` AS
SELECT
  DATE_TRUNC(order_date, MONTH) AS month,
  COUNT(*) AS orders,
  SUM(is_discrepant) AS discrepant_orders,
  SAFE_DIVIDE(SUM(is_discrepant), COUNT(*)) AS discrepant_rate,
  SUM(abs_diff) AS sum_abs_diff
FROM `meli_case.vw_order_recon_per_order`
GROUP BY 1;

--meli_case.vw_discrepancies_by_seller_month
CREATE OR REPLACE VIEW `meli_case.vw_discrepancies_by_seller_month` AS
SELECT
  month,
  seller_id,
  orders,
  discrepant_orders,
  sum_abs_diff,
  SAFE_DIVIDE(discrepant_orders, NULLIF(orders, 0)) AS discrepant_rate,           -- %
  SAFE_DIVIDE(sum_abs_diff,   NULLIF(orders, 0)) AS avg_abs_diff_per_order,       -- R$/pedido
  CASE
    WHEN SAFE_DIVIDE(discrepant_orders, NULLIF(orders, 0)) >= 0.20 THEN 'Alta'
    WHEN SAFE_DIVIDE(discrepant_orders, NULLIF(orders, 0)) >= 0.10 THEN 'Média'
    ELSE 'Baixa'
  END AS risk_bucket
FROM `meli_case.vw_discrepancies_by_seller_month`;

--meli_case.vw_orphans_integration
CREATE OR REPLACE VIEW `meli_case.vw_orphans_integration` AS
WITH bounds AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AS start_dt,
    CURRENT_DATE() AS end_dt
),

-- 1) Pedido SEM fatura emitida
order_no_invoice AS (
  SELECT
    'ORDER_NO_INVOICE'                              AS orphan_type,
    CAST(o.order_id AS STRING)                      AS order_id,
    CAST(NULL AS STRING)                            AS order_item_id,
    CAST(NULL AS STRING)                            AS invoice_id,
    CAST(NULL AS STRING)                            AS invoice_item_id,
    CAST(NULL AS STRING)                            AS payment_id,
    CAST(o.seller_id AS STRING)                     AS seller_id,
    CAST(o.customer_id AS STRING)                   AS customer_id,
    DATE(o.created_at)                              AS occurred_date,
    SAFE_CAST(SUM(IFNULL(oi.unit_price,0)*IFNULL(oi.quantity,0)-IFNULL(oi.discount_amount,0)) AS NUMERIC) AS amount_hint,
    TO_JSON_STRING(STRUCT(o.order_status AS order_status)) AS details_json
  FROM `meli_case.order` o
  LEFT JOIN `meli_case.order_item` oi USING (order_id)
  LEFT JOIN `meli_case.invoice` i
    ON i.order_id = o.order_id
   AND i.invoice_status = 'ISSUED'
  CROSS JOIN bounds b
  WHERE DATE(o.created_at) BETWEEN b.start_dt AND b.end_dt
    AND i.invoice_id IS NULL
  GROUP BY orphan_type, order_id, order_item_id, invoice_id, invoice_item_id,
           payment_id, seller_id, customer_id, occurred_date, details_json
),

-- 2) Pagamento aprovado SEM fatura emitida
payment_no_invoice AS (
  SELECT
    'PAYMENT_NO_INVOICE'                            AS orphan_type,
    CAST(p.order_id AS STRING)                      AS order_id,
    CAST(NULL AS STRING)                            AS order_item_id,
    CAST(NULL AS STRING)                            AS invoice_id,
    CAST(NULL AS STRING)                            AS invoice_item_id,
    CAST(p.payment_id AS STRING)                    AS payment_id,
    CAST(o.seller_id AS STRING)                     AS seller_id,
    CAST(o.customer_id AS STRING)                   AS customer_id,
    DATE(p.paid_at)                                 AS occurred_date,
    SAFE_CAST(p.paid_amount AS NUMERIC)             AS amount_hint,
    TO_JSON_STRING(STRUCT(p.payment_status AS payment_status)) AS details_json
  FROM `meli_case.payment` p
  JOIN `meli_case.order` o USING (order_id)
  LEFT JOIN `meli_case.invoice` i
    ON i.order_id = p.order_id
   AND i.invoice_status = 'ISSUED'
  CROSS JOIN bounds b
  WHERE p.payment_status = 'APPROVED'
    AND DATE(p.paid_at) BETWEEN b.start_dt AND b.end_dt
    AND i.invoice_id IS NULL
),

-- 3) Item do pedido SEM item de NF
order_item_no_invoice_item AS (
  SELECT
    'ORDER_ITEM_NO_INVOICE_ITEM'                    AS orphan_type,
    CAST(oi.order_id AS STRING)                     AS order_id,
    CAST(oi.order_item_id AS STRING)                AS order_item_id,
    CAST(NULL AS STRING)                            AS invoice_id,
    CAST(NULL AS STRING)                            AS invoice_item_id,
    CAST(NULL AS STRING)                            AS payment_id,
    CAST(o.seller_id AS STRING)                     AS seller_id,
    CAST(o.customer_id AS STRING)                   AS customer_id,
    DATE(o.created_at)                              AS occurred_date,
    SAFE_CAST((IFNULL(oi.unit_price,0)*IFNULL(oi.quantity,0)-IFNULL(oi.discount_amount,0)) AS NUMERIC) AS amount_hint,
    TO_JSON_STRING(STRUCT(oi.item_id AS item_id))   AS details_json
  FROM `meli_case.order_item` oi
  JOIN `meli_case.order` o USING (order_id)
  LEFT JOIN `meli_case.invoice_item` ii
    ON ii.order_id = oi.order_id
   AND ii.order_item_id = oi.order_item_id
  LEFT JOIN `meli_case.invoice` i
    ON i.invoice_id = ii.invoice_id
  CROSS JOIN bounds b
  WHERE DATE(o.created_at) BETWEEN b.start_dt AND b.end_dt
    AND i.invoice_status = 'ISSUED'
    AND ii.invoice_item_id IS NULL
),

-- 4) Item de NF SEM item de pedido
invoice_item_no_order_item AS (
  SELECT
    'INVOICE_ITEM_NO_ORDER_ITEM'                    AS orphan_type,
    CAST(ii.order_id AS STRING)                     AS order_id,
    CAST(NULL AS STRING)                            AS order_item_id,
    CAST(ii.invoice_id AS STRING)                   AS invoice_id,
    CAST(ii.invoice_item_id AS STRING)              AS invoice_item_id,
    CAST(NULL AS STRING)                            AS payment_id,
    CAST(o.seller_id AS STRING)                     AS seller_id,
    CAST(o.customer_id AS STRING)                   AS customer_id,
    DATE(i.created_at)                              AS occurred_date,
    SAFE_CAST(ii.net_amount AS NUMERIC)             AS amount_hint,
    TO_JSON_STRING(STRUCT(ii.item_id AS item_id))   AS details_json
  FROM `meli_case.invoice_item` ii
  JOIN `meli_case.invoice` i USING (invoice_id, order_id)
  LEFT JOIN `meli_case.order` o USING (order_id)
  LEFT JOIN `meli_case.order_item` oi
    ON oi.order_id = ii.order_id
   AND oi.order_item_id = ii.order_item_id
  CROSS JOIN bounds b
  WHERE i.invoice_status = 'ISSUED'
    AND DATE(i.created_at) BETWEEN b.start_dt AND b.end_dt
    AND oi.order_item_id IS NULL
),

-- 5) NF emitida SEM linhas de imposto
invoice_no_tax AS (
  SELECT
    'INVOICE_NO_TAX'                                AS orphan_type,
    CAST(i.order_id AS STRING)                      AS order_id,
    CAST(NULL AS STRING)                            AS order_item_id,
    CAST(i.invoice_id AS STRING)                    AS invoice_id,
    CAST(NULL AS STRING)                            AS invoice_item_id,
    CAST(NULL AS STRING)                            AS payment_id,
    CAST(o.seller_id AS STRING)                     AS seller_id,
    CAST(o.customer_id AS STRING)                   AS customer_id,
    DATE(i.created_at)                              AS occurred_date,
    SAFE_CAST(i.total_invoiced AS NUMERIC)          AS amount_hint,
    TO_JSON_STRING(STRUCT(i.issue_date AS issue_date)) AS details_json
  FROM `meli_case.invoice` i
  LEFT JOIN `meli_case.order` o USING (order_id)
  LEFT JOIN `meli_case.invoice_item` ii USING (invoice_id, order_id)
  LEFT JOIN `meli_case.tax_calculation` tc
    ON tc.invoice_id = ii.invoice_id
   AND tc.invoice_item_id = ii.invoice_item_id
  CROSS JOIN bounds b
  WHERE i.invoice_status = 'ISSUED'
    AND DATE(i.created_at) BETWEEN b.start_dt AND b.end_dt
  GROUP BY orphan_type, order_id, order_item_id, invoice_id, invoice_item_id,
           payment_id, seller_id, customer_id, occurred_date, amount_hint, details_json
  HAVING COUNT(tc.tax_type) = 0
)

SELECT * FROM order_no_invoice
UNION ALL
SELECT * FROM payment_no_invoice
UNION ALL
SELECT * FROM order_item_no_invoice_item
UNION ALL
SELECT * FROM invoice_item_no_order_item
UNION ALL
SELECT * FROM invoice_no_tax;

--meli_case.vw_tax_rate_vs_hist_by_jurisdiction
CREATE OR REPLACE VIEW `meli_case.vw_tax_rate_vs_hist_by_jurisdiction` AS
WITH base AS (
  SELECT
    DATE_TRUNC(i.issue_date, MONTH) AS month,
    COALESCE(tc.jurisdiction, 'UNK') AS jurisdiction,
    SUM(tc.tax_amount)   AS tax_amt,
    SUM(ii.net_amount)   AS net_amt
  FROM `meli_case.invoice_item` ii
  JOIN `meli_case.invoice` i
    USING (invoice_id, order_id)
  JOIN `meli_case.tax_calculation` tc
    USING (invoice_id, invoice_item_id)
  WHERE i.invoice_status = 'ISSUED'
  GROUP BY 1,2
),
rates AS (
  SELECT
    month,
    jurisdiction,
    SAFE_DIVIDE(tax_amt, NULLIF(net_amt,0)) AS tax_rate
  FROM base
),
with_hist AS (
  SELECT
    jurisdiction,
    month,
    tax_rate,
    -- média móvel dos 12 meses anteriores (exclui o mês corrente)
    AVG(tax_rate) OVER (
      PARTITION BY jurisdiction
      ORDER BY month
      ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
    ) AS hist_tax_rate
  FROM rates
)
SELECT
  month,
  jurisdiction,
  tax_rate,
  hist_tax_rate,
  (tax_rate - hist_tax_rate)                           AS diff_rate,
  SAFE_DIVIDE(tax_rate - hist_tax_rate, NULLIF(hist_tax_rate,0)) AS pct_change
FROM with_hist;
