# Compliance Alerts — Extração & Análise com Python
> API (simulada/real) → Extração (requests) → Desnormalização (JSON → CSV) → EDA automatizada.  
> Foco C&M: resiliência (retry/backoff/paginação), tolerância a falhas e reprodutibilidade.

## Objetivo
- Integrar com API (simulada/real) usando `requests`.
- Tratar e **desnormalizar** JSON em esquema tabular robusto.
- Persistir em **CSV** e produzir **EDA** (insights rápidos para C&M).
- Aplicar boas práticas: **retry**, **backoff**, **paginação**, tolerância a **schema drift**, **self-test** e **seed**.

## Estrutura
```text
python/
├── extract_compliance_data.py       # Script principal (CLI, simulador, HTTP, EDA, self-test)
├── docs/
│   └── python_diagram.png           # Diagrama da arquitetura da solução
└── data/
    ├── compliance_alerts_YYYYMMDD.csv   # Dados desnormalizados (flattened)
    └── compliance_summary_YYYYMMDD.csv # Resumo EDA (distribuições, média, p95)
```

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

### Instalação de dependência
```bash
pip install requests
```

### Simulado (default) — gera CSVs em ./data
```bash
python python/extract_compliance_data.py --simulate --limit 150 --page-size 50 --out-dir data
```

### Self-test (E2E com asserts; ≥100 linhas e schema esperado)
```bash
python python/extract_compliance_data.py --self-test
```

### API real (se existir)
```bash
python python/extract_compliance_data.py --no-simulate --base-url https://api.mercadolibre.com --limit 200
```

### Output
- [compliance_alerts_20251109.csv](data/compliance_alerts_20251109.csv): dados desnormalizados de alertas
- [compliance_summary_20251109.csv](data/compliance_summary_20251109.csv): resumo com métricas EDA (distribuições, mean, p95)


Exemplo (console):
```text
=== Compliance Alerts — EDA (Resumo) ===
Total: 150
Status: open:65, in_progress:52, closed:33
Tipos: WRONG_TAX_RATE:48, MISSING_INVOICE:43, ...
Impacto: medium:54, high:30, low:20, critical:6
Atribuídos: 126 | Sem dono: 24
Sem resolução: 95
Exposure mean: 16342.78 | p95: 47631.42
```
## Desafios & Mitigação
Rate limit (429) → Retry exponencial (urllib3.Retry) + pausa leve por request.

Intermitência (5xx)/timeouts → retries + timeout configurável; logs de progresso.

Autenticação → adicionar headers/token no HttpClient (ponto de extensão).

Schema drift (JSON variável) → normalize_alert tolera ausências e converte tipos (ex.: string→float).

Paginação/volume → --limit, --page-size e escrita streaming do CSV.

Reprodutibilidade → --seed no simulador.

Confiabilidade → --self-test para validar end-to-end rapidamente.

Decisões priorizam critérios importantes para Compliance & Monitoring: robustez, reprocessabilidade e rapidez de diagnóstico.

## IA - Prompts
Objetivo: Normalização de dados JSON com flatten defensivo
Prompt: Recebo objetos JSON com campos aninhados (ex: assigned_to: {id, name}) e valores como string com separadores (ex: '12.500,00'). Pode sugerir uma função Python para "flatten" e limpeza desses dados?

Objetivo: Geração automática de EDA para CSV's
Prompt: Quero gerar um resumo estatístico automático com pandas para os alertas: distribuição por status, impacto e tipo, além de média e p95 da exposição. Pode me ajudar com isso em uma função reaproveitável?

Objetivo: Diagrama de solução para documentar arquitetura
Prompt: Preciso descrever um pipeline de dados em Python que extrai dados de API (simulada ou real), desnormaliza, gera CSV e EDA. Me ajude a montar um fluxo de dados linear para incluir em um diagrama visual.

Objetivo: Refatoração e organização de script com argparse e boas práticas
Prompt: Estou criando um script CLI para processar alertas de compliance. Quero que ele aceite flags como `--simulate`, `--limit`, `--page-size`, `--seed`, `--out-dir`. Pode revisar se a estrutura do `argparse` está clara e eficiente?

