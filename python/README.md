# Compliance Alerts — Extração & Análise com Python
> API (simulada/real) → Extração (requests) → Desnormalização (JSON → CSV) → EDA automatizada.  
> Foco C&M: resiliência (retry/backoff/paginação), tolerância a falhas e reprodutibilidade.

## Objetivo
- Integrar com API (simulada/real) usando `requests`.
- Tratar e **desnormalizar** JSON em esquema tabular robusto.
- Persistir em **CSV** e produzir **EDA** (insights rápidos para C&M).
- Aplicar boas práticas: **retry**, **backoff**, **paginação**, tolerância a **schema drift**, **self-test** e **seed**.

## Estrutura
/python/
extract_compliance_data.py # script principal (CLI, simulador, HTTP, EDA, self-test)
data/
compliance_alerts_YYYYMMDD.csv # dados desnormalizados
compliance_summary_YYYYMMDD.csv # resumo (EDA)

## Componentes
| Componente | Função |
|---|---|
| `SimulatedComplianceAPI` | Simula `GET /compliance_alerts` (paginado) e `GET /compliance_alerts/{id}` com variações realistas |
| `HttpClient` | `requests.Session` com **Retry** + **backoff** + **timeout** |
| `ComplianceRepository` | Abstrai origem (sim/real), pagina IDs e busca detalhes |
| `normalize_alert()` | **Flatten** defensivo (`assigned_to`→`assigned_to_name`, números “sujos”→float) |
| `eda_summary()` | Métricas: dist. por status/tipo/impacto, sem dono, sem resolução, mean/p95 |

## Diagrama da Solução
![Diagrama](docs/python_diagram.png)

## Execução
# deps
pip install requests

# Simulado (default) — gera CSVs em ./data
python python/extract_compliance_data.py --simulate --limit 150 --page-size 50 --out-dir data

# Self-test (E2E com asserts; ≥100 linhas e schema esperado)
python python/extract_compliance_data.py --self-test

# API real (se existir)
python python/extract_compliance_data.py --no-simulate --base-url https://api.mercadolibre.com --limit 200

## Output
- [compliance_alerts_20251108.csv](data/compliance_alerts_20251108.csv): dados desnormalizados de alertas
- [compliance_summary_20251108.csv](data/compliance_summary_20251108.csv): resumo com métricas EDA (distribuições, mean, p95)


Exemplo (console):

=== Compliance Alerts — EDA (Resumo) ===
Total: 150
Status: open:65, in_progress:52, closed:33
Tipos: WRONG_TAX_RATE:48, MISSING_INVOICE:43, ...
Impacto: medium:54, high:30, low:20, critical:6
Atribuídos: 126 | Sem dono: 24
Sem resolução: 95
Exposure mean: 16342.78 | p95: 47631.42
🚧 Desafios comuns de APIs & Mitigação
Rate limit (429) → Retry exponencial (urllib3.Retry) + pausa leve por request.

Intermitência (5xx)/timeouts → retries + timeout configurável; logs de progresso.

Autenticação → adicionar headers/token no HttpClient (ponto de extensão).

Schema drift (JSON variável) → normalize_alert tolera ausências e converte tipos (ex.: string→float).

Paginação/volume → --limit, --page-size e escrita streaming do CSV.

Reprodutibilidade → --seed no simulador.

Confiabilidade → --self-test para validar end-to-end rapidamente.

Decisões priorizam critérios importantes para Compliance & Monitoring: robustez, reprocessabilidade e rapidez de diagnóstico.
