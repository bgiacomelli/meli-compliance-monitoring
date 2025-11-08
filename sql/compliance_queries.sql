-- 2) TABELA DE AUDITORIA REPROCESSÁVEL (Snapshot EOD de Itens)
-- ================================================================================================
CREATE TABLE IF NOT EXISTS `meli_case.audit_item_daily` (
  snapshot_date   DATE          NOT NULL,
  item_id         STRING        NOT NULL,
  seller_id       STRING        NOT NULL,
  category_id     STRING        NOT NULL,
  price           NUMERIC       NOT NULL,
  status          STRING        NOT NULL,
  run_id          STRING        NOT NULL,   -- rastreabilidade
  inserted_at     TIMESTAMP     NOT NULL,
  row_hash        STRING        NOT NULL    -- detecção de mudanças idempotentes
)
PARTITION BY snapshot_date
CLUSTER BY item_id, seller_id, category_id
OPTIONS (description = 'Snapshot diário EOD (reprocessável) de preço/status de Item.');

-- ================================================================================================
-- 3) CONSULTAS DE DETECÇÃO DE RISCO
-- ================================================================================================

-- 3.1) Discrepâncias: Top 10 pedidos com maior diferença |Total Pago - Total Faturado| no último trimestre.
-- Observação (por quê): pagamentos podem incluir refunds/chargebacks; filtrei somente payment_status = 'APPROVED'.
WITH last_quarter_bounds AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 0 MONTH), MONTH) AS month_start_now,
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH) AS month_start_3mo
),
orders_in_window AS (
  SELECT o.order_id
  FROM `meli_case.order` o
  JOIN last_quarter_bounds b
    ON DATE(o.created_at) >= b.month_start_3mo
   AND DATE(o.created_at) <  b.month_start_now
),
payments AS (
  SELECT p.order_id, SUM(p.paid_amount) AS total_paid
  FROM `meli_case.payment` p
  JOIN orders_in_window w USING (order_id)
  WHERE p.payment_status = 'APPROVED'
  GROUP BY p.order_id
),
invoices AS (
  SELECT i.order_id, SUM(i.total_invoiced) AS total_invoiced
  FROM `meli_case.invoice` i
  JOIN orders_in_window w USING (order_id)
  WHERE i.invoice_status IN ('ISSUED') -- somente faturadas válidas
  GROUP BY i.order_id
)
SELECT
  o.order_id,
  COALESCE(p.total_paid, 0)     AS total_pago,
  COALESCE(i.total_invoiced, 0) AS total_faturado,
  COALESCE(p.total_paid, 0) - COALESCE(i.total_invoiced, 0) AS diferenca,
  ABS(COALESCE(p.total_paid, 0) - COALESCE(i.total_invoiced, 0)) AS abs_diferenca
FROM orders_in_window o
LEFT JOIN payments p USING (order_id)
LEFT JOIN invoices i USING (order_id)
ORDER BY abs_diferenca DESC
LIMIT 10;

-- 3.2) Anomalias em Impostos por Categoria:
--  - taxa média no mês passado vs média histórica do ano anterior (12 meses anteriores ao mês passado).
--  - Top 3 categorias por maior desvio absoluto.
-- Observação (por quê): taxa efetiva agregada por categoria a partir de tax_calculation / invoice_item.net_amount.
WITH date_refs AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS last_month_start,
    DATE_TRUNC(CURRENT_DATE(), MONTH) AS this_month_start
),
-- Base do último mês
lm AS (
  SELECT
    ii.item_id,
    it.category_id,
    SUM(tc.tax_amount) AS lm_tax_amount,
    SUM(ii.net_amount) AS lm_net_amount
  FROM `meli_case.invoice_item` ii
  JOIN `meli_case.invoice` i USING (invoice_id, order_id)
  JOIN `meli_case.tax_calculation` tc USING (invoice_id, invoice_item_id)
  JOIN `meli_case.item` it USING (item_id)
  JOIN date_refs d
    ON i.issue_date >= d.last_month_start
   AND i.issue_date <  d.this_month_start
  WHERE i.invoice_status = 'ISSUED'
  GROUP BY 1,2
),
lm_cat AS (
  SELECT
    category_id,
    SAFE_DIVIDE(SUM(lm_tax_amount), SUM(lm_net_amount)) AS lm_tax_rate
  FROM lm
  GROUP BY 1
),
-- Janela histórica: 12 meses anteriores ao último mês
hist AS (
  SELECT
    ii.item_id,
    it.category_id,
    SUM(tc.tax_amount) AS h_tax_amount,
    SUM(ii.net_amount) AS h_net_amount
  FROM `meli_case.invoice_item` ii
  JOIN `meli_case.invoice` i USING (invoice_id, order_id)
  JOIN `meli_case.tax_calculation` tc USING (invoice_id, invoice_item_id)
  JOIN `meli_case.item` it USING (item_id)
  JOIN date_refs d
    ON i.issue_date >= DATE_SUB(d.last_month_start, INTERVAL 12 MONTH)
   AND i.issue_date <  d.last_month_start
  WHERE i.invoice_status = 'ISSUED'
  GROUP BY 1,2
),
hist_cat AS (
  SELECT
    category_id,
    SAFE_DIVIDE(SUM(h_tax_amount), SUM(h_net_amount)) AS hist_tax_rate
  FROM hist
  GROUP BY 1
)
SELECT
  COALESCE(c.category_id, h.category_id) AS category_id,
  cat.category_name,
  c.lm_tax_rate,
  h.hist_tax_rate,
  (COALESCE(c.lm_tax_rate, 0) - COALESCE(h.hist_tax_rate, 0)) AS diff_rate,
  SAFE_DIVIDE(COALESCE(c.lm_tax_rate, 0) - COALESCE(h.hist_tax_rate, 0), NULLIF(h.hist_tax_rate, 0)) AS pct_change
FROM lm_cat c
FULL OUTER JOIN hist_cat h USING (category_id)
LEFT JOIN `meli_case.category` cat ON cat.category_id = COALESCE(c.category_id, h.category_id)
ORDER BY ABS(diff_rate) DESC
LIMIT 3;

-- ================================================================================================
-- 4) FUNÇÃO E PROCEDURE REPROCESSÁVEL (AUDITORIA DIÁRIA DE ITENS)
-- ================================================================================================
-- Helper: função para hash da linha (para controle de mudança)
CREATE OR REPLACE FUNCTION `meli_case.fn_row_hash_item_audit`(
  item_id STRING, seller_id STRING, category_id STRING, price NUMERIC, status STRING, snapshot_date DATE
) AS (
  TO_HEX(SHA256(TO_JSON_STRING(STRUCT(item_id, seller_id, category_id, price, status, snapshot_date))))
);

-- Procedure reprocessável
CREATE OR REPLACE PROCEDURE `meli_case.sp_audit_item_daily`(p_snapshot_date DATE, p_run_id STRING)
BEGIN
  DECLARE cutoff_ts TIMESTAMP DEFAULT
    -- fim do dia local (23:59:59) em America/Sao_Paulo
    TIMESTAMP(DATETIME(p_snapshot_date) + INTERVAL 1 DAY - INTERVAL 1 SECOND, "America/Sao_Paulo");

  -- 2) Idempotência
  DELETE FROM `meli_case.audit_item_daily`
   WHERE snapshot_date = p_snapshot_date;

  -- 3) Reconstrução EOD a partir do SCD
  INSERT INTO `meli_case.audit_item_daily`
    (snapshot_date, item_id, seller_id, category_id, price, status, run_id, inserted_at, row_hash)
  WITH eod AS (
    SELECT
      s.item_id,
      s.seller_id,
      s.category_id,
      s.price,
      s.status
    FROM `meli_case.item_scd` s
    WHERE s.valid_from <= cutoff_ts
      AND (s.valid_to IS NULL OR s.valid_to > cutoff_ts)
  )
  SELECT
    p_snapshot_date,
    e.item_id,
    e.seller_id,
    e.category_id,
    e.price,
    e.status,
    p_run_id,
    CURRENT_TIMESTAMP(),
    `meli_case`.fn_row_hash_item_audit(e.item_id, e.seller_id, e.category_id, e.price, e.status, p_snapshot_date)
  FROM eod e;
END;


-- Exemplo de execução:
-- CALL `meli_case.sp_audit_item_daily`(DATE '2025-10-31', GENERATE_UUID());

/***************************************************************************************************
 *                         NOTAS DE DESIGN / COMPLIANCE (RESUMO)
 *  - Particionamento: datas de negócio para reduzir varreduras/custo; clusterização por chaves de join.
 *  - Identidade fiscal via `party`: centraliza CPF/CNPJ, facilita KYC/AML e cruzamentos em invoices.
 *  - Taxa efetiva: soma(tax_amount)/soma(net_amount) evita viés de média simples.
 *  - Discrepâncias: pagamentos aprovados vs faturas emitidas; considere views para refunds/chargebacks.
 *  - Reprocessabilidade: garantida por SCD; procedure é idempotente e determinística por data.
 *  - Limitações: sem SCD não há reconstrução histórica perfeita; garantir captura de mudanças.
 ***************************************************************************************************/
