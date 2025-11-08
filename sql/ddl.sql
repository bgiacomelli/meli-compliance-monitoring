-- ================================================================================================
-- 0) DATASET
-- ================================================================================================
CREATE SCHEMA IF NOT EXISTS `meli_case`;

-- ================================================================================================
-- 1) TABELAS (DDL)
-- ================================================================================================
-- Entidade geral para comprador/vendedor (Party)
CREATE TABLE IF NOT EXISTS `meli_case.party` (
  party_id        STRING        NOT NULL,
  party_type      STRING        NOT NULL, -- 'PERSON' | 'COMPANY'
  tax_id_type     STRING        NOT NULL, -- 'CPF' | 'CNPJ' | 'EIN' | 'VAT' | 'OTHER'
  tax_id          STRING        NOT NULL, -- documento fiscal
  country_code    STRING,                 -- 'BR', etc.
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
OPTIONS (description = 'Pessoa Física/Jurídica com identificação fiscal');

-- Especializações (1:1 com party)
CREATE TABLE IF NOT EXISTS `meli_case.customer` (
  customer_id     STRING        NOT NULL,
  party_id        STRING        NOT NULL,
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
CLUSTER BY party_id
OPTIONS (description = 'Cliente (comprador).');

CREATE TABLE IF NOT EXISTS `meli_case.seller` (
  seller_id       STRING        NOT NULL,
  party_id        STRING        NOT NULL,
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
CLUSTER BY party_id
OPTIONS (description = 'Vendedor (merchant).');

CREATE TABLE IF NOT EXISTS `meli_case.category` (
  category_id     STRING        NOT NULL,
  category_name   STRING        NOT NULL,
  parent_id       STRING,
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
OPTIONS (description = 'Categorias de item (hierárquicas).');

-- Item atual (estado corrente)
CREATE TABLE IF NOT EXISTS `meli_case.item` (
  item_id         STRING        NOT NULL,
  seller_id       STRING        NOT NULL,
  category_id     STRING        NOT NULL,
  title           STRING,
  status          STRING        NOT NULL, -- 'ACTIVE' | 'PAUSED' | 'BLOCKED' | 'DELISTED'
  price           NUMERIC       NOT NULL, -- preço corrente
  currency        STRING        NOT NULL, -- 'BRL'
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
CLUSTER BY seller_id, category_id
OPTIONS (description = 'Item com estado corrente.');

-- SCD de Item para auditoria (reprocessável)
CREATE TABLE IF NOT EXISTS `meli_case.item_scd` (
  item_id         STRING        NOT NULL,
  seller_id       STRING        NOT NULL,
  category_id     STRING        NOT NULL,
  status          STRING        NOT NULL,
  price           NUMERIC       NOT NULL,
  currency        STRING        NOT NULL,
  valid_from      TIMESTAMP     NOT NULL,
  valid_to        TIMESTAMP,              -- exclusivo; NULL = aberto
  is_current      BOOL           NOT NULL
)
PARTITION BY DATE(valid_from)
CLUSTER BY item_id, seller_id, category_id
OPTIONS (description = 'Tipo 2: intervalos de vigência do item (preço/status).');

-- Pedido e linhas
CREATE TABLE IF NOT EXISTS `meli_case.order` (
  order_id        STRING        NOT NULL,
  customer_id     STRING        NOT NULL,
  seller_id       STRING        NOT NULL,
  order_status    STRING        NOT NULL, -- 'PLACED' | 'CANCELLED' | 'FULFILLED' ...
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY customer_id, seller_id
OPTIONS (description = 'Pedidos.');

CREATE TABLE IF NOT EXISTS `meli_case.order_item` (
  order_id        STRING        NOT NULL,
  order_item_id   STRING        NOT NULL,
  item_id         STRING        NOT NULL,
  quantity        INT64         NOT NULL,
  unit_price      NUMERIC       NOT NULL, -- preço no momento do pedido (sem imposto)
  discount_amount NUMERIC       DEFAULT 0,
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY order_id, item_id
OPTIONS (description = 'Linhas de pedido.');

-- Pagamentos (para "Valor Total Pago")
CREATE TABLE IF NOT EXISTS `meli_case.payment` (
  payment_id      STRING        NOT NULL,
  order_id        STRING        NOT NULL,
  paid_amount     NUMERIC       NOT NULL,
  currency        STRING        NOT NULL,
  payment_status  STRING        NOT NULL, -- 'APPROVED','REFUNDED','CHARGEBACK','PENDING'
  paid_at         TIMESTAMP     NOT NULL
)
PARTITION BY DATE(paid_at)
CLUSTER BY order_id, payment_status
OPTIONS (description = 'Pagamentos efetuados por pedido.');

-- Fatura e linhas
CREATE TABLE IF NOT EXISTS `meli_case.invoice` (
  invoice_id      STRING        NOT NULL,
  order_id        STRING        NOT NULL,
  buyer_party_id  STRING        NOT NULL, -- normalmente customer.party_id
  seller_party_id STRING        NOT NULL, -- seller.party_id
  issue_date      DATE          NOT NULL,
  invoice_status  STRING        NOT NULL, -- 'ISSUED','VOIDED','PENDING'
  total_invoiced  NUMERIC       NOT NULL, -- total com impostos
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
PARTITION BY issue_date
CLUSTER BY order_id, buyer_party_id, seller_party_id, invoice_status
OPTIONS (description = 'Faturas emitidas.');

CREATE TABLE IF NOT EXISTS `meli_case.invoice_item` (
  invoice_id      STRING        NOT NULL,
  invoice_item_id STRING        NOT NULL,
  order_id        STRING        NOT NULL,
  order_item_id   STRING        NOT NULL,
  item_id         STRING        NOT NULL,
  quantity        INT64         NOT NULL,
  net_amount      NUMERIC       NOT NULL, -- base sem imposto
  gross_amount    NUMERIC       NOT NULL, -- com imposto
  created_at      TIMESTAMP     NOT NULL,
  updated_at      TIMESTAMP     NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY invoice_id, order_id, item_id
OPTIONS (description = 'Linhas de fatura.');

-- Cálculo de impostos por linha (um ou mais impostos por linha)
CREATE TABLE IF NOT EXISTS `meli_case.tax_calculation` (
  invoice_id      STRING        NOT NULL,
  invoice_item_id STRING        NOT NULL,
  tax_type        STRING        NOT NULL, -- ex: ICMS, IPI, PIS, COFINS, ISS
  taxable_base    NUMERIC       NOT NULL, -- base de cálculo
  tax_rate        NUMERIC       NOT NULL, -- taxa aplicada (ex.: 0.18)
  tax_amount      NUMERIC       NOT NULL, -- valor de imposto
  jurisdiction    STRING,                 -- UF, município, etc.
  created_at      TIMESTAMP     NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY invoice_id, tax_type
OPTIONS (description = 'Impostos aplicados por linha de fatura.');
