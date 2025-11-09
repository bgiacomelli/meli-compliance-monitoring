Case Técnico Mercado Livre — Modelagem, SQL e Python para Compliance & Monitoring (BigQuery)

# Compliance & Monitoring

> Modelo e SQL para monitorar **faturação/impostos** e **reconciliação Pago × Faturado** no BigQuery.
> Entregáveis: **DER documentado**, **DDL** e **consultas** (discrepâncias, anomalias fiscais, snapshot reprocessável).

---

## 1) Escopo & alinhamento com C&M

- **Objetivo fiscal**: garantir que **faturas** reflitam a transação e **impostos** sejam calculados corretamente (nível de item e de imposto).
- **Objetivo financeiro**: reconciliar **pagamentos** versus **faturamento** por pedido; diferença esperada ≈ 0.
- **Objetivo de auditoria**: reconstruir, de forma **reprocessável**, o **preço** e **status** dos itens no **EOD**.

---

## 2) DER — Design e decisões

### Entidades principais
- **party**: raiz de identidade fiscal (CPF/CNPJ, país). **Por quê?** Centraliza KYC/AML e evita duplicidade ao modelar papéis.
- **customer / seller**: especializações de `party` (papéis de comprador/vendedor).
- **category**: hierárquica (suporta *parent_id*).
- **item**: estado corrente; **item_scd**: histórico **SCD Tipo 2** (intervalos [valid_from, valid_to)).
- **order / order_item**: pedido e suas linhas (preço no momento da venda).
- **invoice / invoice_item**: faturação fiscal (valor líquido e bruto por linha).
- **tax_calculation**: 1..N impostos por **linha da fatura** (tax_type, base, taxa, valor).
- **payment**: movimentos financeiros aprovados por pedido.
- **audit_item_daily**: **snapshot EOD** reprocessável de (item_id, seller_id, category_id, price, status).

### Racional do layout
- **Centro = `order`** para ancorar reconciliação.
- **Eixo fiscal (direita)**: `invoice` → `invoice_item` → `tax_calculation` (grão fiscal).
- **Eixo comercial (esquerda)**: `order_item` → `item` → `category` (grão de venda).
- **Topo (identidade)**: `party` com `customer`/`seller`.
- **Rodapé (movimentos/estado)**: `payment`, `item_scd`, `audit_item_daily`.

### Chaves/grãos (lógicos)
- `order`: 1 linha/pedido.
- `order_item`: 1 linha/(pedido,item_id,order_item_id).
- `invoice_item`: 1 linha/(invoice_id,invoice_item_id) (referencia `order_item`).
- `tax_calculation`: 1 linha/(invoice_id,invoice_item_id,tax_type).
- `item_scd`: 1 linha/(item_id,valid_from).
- `audit_item_daily`: 1 linha/(snapshot_date,item_id).

> Nota: BigQuery comum não aplica PK/FK. Chaves são **lógicas**; integridade é tratada por **checks** e processos.

---

## 3) Star schema (consumo/BI)

- **Fato Fiscal**: `f_invoice_item` (grão invoice_item) e `f_tax` (imposto por linha).
- **Fato Financeiro/Comercial**: `f_order`, `f_order_item`, `f_payment` e view `f_order_recon` (pagos × faturados).
- **Dimensões conformadas**: `dim_party`, `dim_customer`, `dim_seller`, `dim_category`, `dim_item_scd` (SCD2), `dim_date`.
- **Por quê**: separa análises **fiscais** (taxa efetiva, composição por imposto) de **reconciliação** (diferenças por pedido), reduzindo complexidade de joins e custo.

---

## 4) Consultas do case — lógica, hipóteses e métricas

### 4.1 Discrepâncias Pago × Faturado (último trimestre)
- **Janela**: dos **3 meses anteriores** até o **início do mês atual**.
- **Pago**: soma de `payment.paid_amount` somente `payment_status='APPROVED'`.
- **Faturado**: soma de `invoice.total_invoiced` somente `invoice_status='ISSUED'`.
- **Saída**: `order_id`, `total_paid`, `total_invoiced`, `diff` e `abs_diff`. Ordenado por `abs_diff` (Top-10).
- **Suposições**:
  - Reembolsos/chargebacks não entram (não há reversão de fatura; se existirem, tratá-los numa *view* específica).
  - Multimoeda fora de escopo (usamos `currency='BRL'`).

### 4.2 Anomalias de impostos por categoria
- **Taxa efetiva** por categoria = `SUM(tax_amount)/SUM(net_amount)`.
- **Períodos**:
  - `last_month`: mês imediatamente anterior ao atual.
  - `hist`: **12 meses** antes de `last_month`.
- **Saída**: `category_id`, `category_name`, `lm_tax_rate`, `hist_tax_rate`, `diff_rate`, `pct_change` (Top-3 por `ABS(diff_rate)`).
- **Suposições**:
  - Consideramos apenas faturas `ISSUED`.
  - Mitigação de divisão por zero via `SAFE_DIVIDE`.
  - (Opcional) filtro de **volume mínimo** por categoria (ex.: `SUM(net_amount) >= X`) para reduzir falsos positivos.

### 4.3 Snapshot EOD reprocessável (procedure)
- **Ponto de corte**: `23:59:59` local `America/Sao_Paulo` do `p_snapshot_date`.
- **Fonte de verdade**: `item_scd` com vigências **[valid_from, valid_to)**.
- **Idempotência**: `DELETE` do snapshot do dia antes de inserir; **hash** de linha (`fn_row_hash_item_audit`) para auditoria.
- **Vantagem**: qualquer dia pode ser **recalculado** sem efeitos colaterais.
- **Limitação**: depende da **completude** do SCD; mudanças não capturadas inviabilizam reconstrução.

---

## 5) Otimizações BigQuery aplicadas

- **Particionamento**:
  - `order`, `order_item` por data de criação; `invoice` por `issue_date`;
  - `payment` por `paid_at`; `item_scd` por `valid_from`; `audit_item_daily` por `snapshot_date`.
- **Clusterização**:
  - Por chaves de *join* frequentes (`order_id`, `item_id`, `category_id`, etc.) para reduzir *shuffle* e custo.
- **Funções e CTEs**:
  - `SAFE_DIVIDE`, `COALESCE` e janelas de data via `DATE_TRUNC`.
- **Design das queries**:
  - Agregações no menor conjunto possível (ex.: sumarizar `payment`/`invoice` por pedido antes do *join*).
  - Evitar *cartesian products*; `JOIN USING()` quando possível.
- **Tipos numéricos**:
  - Literais de taxa tipados como `NUMERIC` para compatibilidade com valores monetários (`NUMERIC`).

---

## 6) Qualidade de dados (sem PK/FK físicas)

- **Chaves lógicas** e **views de violação** (ex.: duplicidade em `invoice_item`, órfãos em `tax_calculation`).
- **ASSERTs** opcionais para pipeline (param a execução em caso de inconsistência).
- **Deduplicação** recomendada em ingestão (MERGE por chaves de negócio).

---

## 7) Hipóteses & decisões (bloqueios, trade-offs)

- **Moeda**: BRL; sem conversão cambial.
- **Taxas**: impostas por categoria para simplificar; em produção viriam de um motor fiscal (NCM/UF/CFOP).
- **Reembolsos**: fora do escopo; poderiam entrar como fatos negativos/ajustes em views de reconciliação.
- **Timezone**: `America/Sao_Paulo` para EOD (implica cuidado em horários de verão históricos).
- **Sem constraints físicas**: aceito no BigQuery; qualidade garantida por processos e checagens.
- **Granularidade**:
  - Fiscal no **nível de linha de fatura** (necessário para auditoria tributária).
  - Reconciliação no **nível de pedido** (comparabilidade direta com pagamento).

---

## 8) Entregáveis

- **DER**: `der/model.dbml` + `der/der.png` (layout: party topo; order centro; comercial à esquerda; fiscal à direita; movimentos/estado no rodapé).  
- **DDL + Consultas + Procedure**: `sql/compliance_queries.sql`  
  - DDL de todas as tabelas (`meli_case.*`).  
  - **3 consultas**: discrepâncias (3.1), anomalias fiscais (3.2), procedure + função de hash (4).  
  - Comentários inline explicando hipóteses e porquês.
- **(Opcional)** Dados sintéticos p/ demonstração: `sql/seed_demo_data_no_constraints_lpad.sql`.  
- **(Opcional)** Views de consumo `star_schema_views.sql` e `f_order_recon`.

---

## 9) Efetividade das queries (resumo)

- **Discrepâncias**: restrição às partições certas (datas do último trimestre), pré-agregação por pedido e uso de `COALESCE` para casos sem fatura/pagamento.  
- **Anomalias**: cálculo de **taxa efetiva** (média ponderada por base tributável) em vez de média simples; comparação com janela **histórica de 12m**.  
- **Snapshot**: **idempotente** e **determinístico**; “as-of join” implícito via vigências do SCD.

---

## 10) IA usada — Ferramentas, como utilizei e prompts (papel de apoio sênior)

**Ferramenta:** ChatGPT.  

### Como utilizei (resumo)
- **Revisão & otimização de SQL:** alternativas de join/pre-agregação e estimativa qualitativa de custo (scan).
- **Compatibilidade BigQuery:** conversões de dialeto, funções e **tipagem NUMERIC** consistente.
- **Boilerplate:** esqueleto DDL comentado, views star schema e templates de `ASSERT`/anti-join.
- **Dados de teste:** seed determinístico com **anomalias controladas**.
- **Documentação:** lapidar hipóteses/limitações e checklist de qualidade.

### Prompts e aplicação no case

#### Modelagem & DER
- **Prompt:** “Dado este DER (texto/DBML), critique a granularidade dos fatos e a adoção de Party/Role. Onde posso simplificar joins sem perder rastreabilidade fiscal?”  
  **Aplicação:** validação do **Party/Role** e dos grãos (`invoice_item`, `tax_calculation`, `order`, `order_item`).  
  **Saída/Impacto:** DER final + notas de rastreabilidade e chaves lógicas.

#### BigQuery & performance
- **Prompt:** “Reescreva esta query pré-agregando antes dos joins para reduzir bytes escaneados. Explique impacto em custo/latência.”  
  **Aplicação:** pré-agregações em pagamentos/faturas por `order_id` na query de discrepâncias.  
  **Saída/Impacto:** menos shuffle/scan; queries mais baratas.
    
- **Prompt:** “Converta para BigQuery Standard (MOD, LPAD, NUMERIC). Aponte mismatches de tipo prováveis.”  
  **Aplicação:** substituição de `%`→`MOD()`, `FORMAT`→`LPAD/CONCAT`, cast de literais de taxa para **NUMERIC**.  
  **Saída/Impacto:** erros de parser/tipo eliminados.

#### Discrepâncias (Pago × Faturado)
- **Prompt:** “Revise esta query de reconciliação: onde uso partição para limitar scan ao último trimestre e evitar full table scan?”  
  **Aplicação:** janelas baseadas em `DATE_TRUNC` + filtros de partição em `order.created_at`.  
  **Saída/Impacto:** Top-10 discrepâncias eficiente e estável.

#### Impostos por categoria
- **Prompt:** “Quero taxa efetiva (média ponderada). Dê versão com FULL OUTER para categorias só em um período + filtro de volume mínimo.”  
  **Aplicação:** cálculo `SUM(tax_amount)/SUM(net_amount)` + opção FULL OUTER e `SAFE_DIVIDE`; filtro opcional por volume.  
  **Saída/Impacto:** detecção robusta de anomalias, menos falsos positivos.
- **Prompt:** “Liste armadilhas de média simples e por que usar ponderação.”  
  **Aplicação:** documentação das hipóteses e da métrica.

#### Seed sintético (dados de teste)
- **Prompt:** “Gere seed determinístico com discrepâncias controladas no último trimestre e alíquotas 25% (Books), 1% (Games) no último mês.”  
  **Aplicação:** script `seed_demo_data_*.sql` com CTEs/arrays, evitando `DECLARE` no meio.  
  **Saída/Impacto:** dados realistas para dashboard e validação das queries.

#### Procedure reprocessável
- **Prompt:** “Compare DELETE+INSERT vs MERGE para snapshot EOD. Qual mais idempotente/barato no BigQuery? Dê exemplo minimalista.”  
  **Aplicação:** escolha por **DELETE+INSERT** (simples, determinístico) + hash de linha; EOD em `America/Sao_Paulo`.  
  **Saída/Impacto:** `sp_audit_item_daily` idempotente e reprocessável.

#### Debug & compatibilidade
- **Prompt:** “Explique erro FLOAT64 → NUMERIC; onde tipar literais (CAST(0.25 AS NUMERIC)).”  
  **Aplicação:** uniformização de tipos monetários.  
  **Saída/Impacto:** inserções em `invoice_item`/`tax_calculation` sem falhas.
- **Prompt:** “Erros de parser com % e FORMAT. Versão 100% compatível com MOD e LPAD.”  
  **Aplicação:** padronização do seed e das funções.

#### Documentação & revisão
- **Prompt:** “Edite README para concisão e foco C&M: objetivos, hipóteses, limitações, otimizações BigQuery.”  
  **Aplicação:** seção de hipóteses/limitações e checklist de qualidade.  
  **Saída/Impacto:** documentação clara e auditável.
- **Prompt:** “Dê checklist final (particionamento, clusterização, SAFE_DIVIDE, anti-join de órfãos, janelas corretas).”  
  **Aplicação:** verificação de sanidade antes da entrega.

> **Observação:** em todos os casos, as saídas foram **revisadas e ajustadas** por mim, com testes e validação de custos/compatibilidade no ambiente.
