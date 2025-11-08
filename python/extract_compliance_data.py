# Proposta: Varredura (API simulada/real), desnormalização e CSV + EDA para Compliance (C&M).

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -----------------------------
# Config & Helpers
# -----------------------------

@dataclass(frozen=True)
class Settings:
    base_url: str
    simulate: bool
    limit: int
    page_size: int
    out_dir: str
    seed: int
    request_timeout_sec: int = 10
    max_retries: int = 3
    backoff_factor: float = 0.5           # por quê: resiliência a 429/5xx
    rate_limit_per_sec: float = 5.0       # por quê: evitar bursts (APIs reais)

def iso_now_date() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def sleep_for_rate_limit(rate_per_sec: float) -> None:
    if rate_per_sec > 0:
        time.sleep(1.0 / rate_per_sec)


# -----------------------------
# HTTP Client (requests)
# -----------------------------

class HttpClient:
    """requests.Session com Retry/backoff/timeout (GET)."""
    def __init__(self, base_url: str, timeout_sec: int, max_retries: int, backoff_factor: float):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        retry = Retry(
            total=max_retries, read=max_retries, connect=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self.session.get(url, params=params, timeout=self.timeout_sec)
        try:
            payload = resp.json() if resp.content else {}
        except Exception:
            payload = {"_raw_text": resp.text}  # por quê: payload pode não ser JSON em erros
        return resp.status_code, payload


# -----------------------------
# Simulated API (deterministic)
# -----------------------------

class SimulatedComplianceAPI:
    """
    Simula:
      GET /compliance_alerts?status=open&limit=...&offset=...
      GET /compliance_alerts/{alert_id}
    """
    TYPES = ["MISSING_INVOICE", "WRONG_TAX_RATE", "INVOICE_AMOUNT_MISMATCH", "TAX_JURISDICTION_ERROR"]
    IMPACT = ["low", "medium", "high", "critical"]
    CATEGORIES = ["Electronics", "Books", "Home", "Games", "Beauty"]
    TAX_CODES = ["ICMS", "IPI", "PIS", "COFINS", "ISS"]

    def __init__(self, seed: int):
        self.seed = seed

    def list_alert_ids(self, status: str, limit: int, offset: int) -> Dict[str, Any]:
        base_rng = random.Random(hash((self.seed, status)) & 0xFFFFFFFF)
        total = max(limit + offset, 200)  # garante 200+ disponíveis
        ids = [f"ALRT-{base_rng.randint(10_000, 99_999)}-{i}" for i in range(offset, min(offset + limit, total))]
        return {"status": status, "count": len(ids), "total": total, "data": ids}

    def get_alert_detail(self, alert_id: str) -> Dict[str, Any]:
        h = abs(hash((self.seed, alert_id)))
        rng = random.Random(h)

        created = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=rng.randint(0, 120), hours=rng.randint(0, 23))
        is_closed = rng.random() < 0.35
        resolution_date = (created + dt.timedelta(days=rng.randint(1, 30))) if is_closed else None

        assigned_to = None if rng.random() < 0.10 else {"id": f"USR-{rng.randint(1000, 9999)}",
                                                        "name": rng.choice(["Ana", "Bruno", "Carla", "Diego", "Eva", "Felipe"])}

        payload = {
            "alert_id": alert_id,
            "type_of_alert": rng.choice(self.TYPES),
            "status": "closed" if is_closed else rng.choice(["open", "in_progress"]),
            "assigned_to": assigned_to,
            "creation_date": created.isoformat(),
            "resolution_date": resolution_date.isoformat() if resolution_date else None,
            "impact_level": rng.choices(self.IMPACT, weights=[4, 3, 2, 1])[0],
            "sla_hours": rng.choice([24, 48, 72, 168]),
            "jurisdiction": rng.choice(["BR-SP", "BR-RJ", "BR-MG", "BR-RS", "BR-PR"]),
            "category": rng.choice(self.CATEGORIES),
            "tax_code": rng.choice(self.TAX_CODES),
            "monetary_exposure": round(rng.uniform(0, 50_000), 2),
            "has_invoice_linked": rng.random() < 0.7,
            "order_id": None if rng.random() < 0.2 else f"O{rng.randint(10_000, 99_999)}",
            "invoice_id": None,
        }
        if payload["has_invoice_linked"] and rng.random() < 0.85:
            payload["invoice_id"] = f"INV-{rng.randint(10_000, 99_999)}"

        if rng.random() < 0.05:
            payload["monetary_exposure"] = str(payload["monetary_exposure"])  # sujar tipo para testar normalização

        return payload


# -----------------------------
# Repository (switch sim/real)
# -----------------------------

class ComplianceRepository:
    """Abstrai origem (real/simulada) e aplica políticas de resiliência."""
    def __init__(self, settings: Settings):
        self.settings = settings
        self.sim = SimulatedComplianceAPI(settings.seed) if settings.simulate else None
        self.http = None if settings.simulate else HttpClient(
            base_url=settings.base_url,
            timeout_sec=settings.request_timeout_sec,
            max_retries=settings.max_retries,
            backoff_factor=settings.backoff_factor,
        )

    def list_ids(self, status: str, limit: int, offset: int) -> List[str]:
        if self.sim:
            payload = self.sim.list_alert_ids(status=status, limit=limit, offset=offset)
        else:
            code, payload = self.http.get("/compliance_alerts", params={"status": status, "limit": limit, "offset": offset})
            if code == 429 or code >= 500:
                raise RuntimeError(f"Upstream error {code}: {payload}")
        return [str(x) for x in payload.get("data", [])]

    def get_detail(self, alert_id: str) -> Dict[str, Any]:
        attempts = 0
        last_err: Optional[Exception] = None
        while attempts < (self.settings.max_retries + 1):
            try:
                if self.sim:
                    return self.sim.get_alert_detail(alert_id)
                code, payload = self.http.get(f"/compliance_alerts/{alert_id}")
                if code == 200:
                    return payload
                if code == 404:
                    return {}
                raise RuntimeError(f"HTTP {code}: {payload}")
            except Exception as e:
                last_err = e
                time.sleep(self.settings.backoff_factor * (2 ** attempts))
                attempts += 1
        raise RuntimeError(f"Failed to fetch {alert_id}: {last_err}")


# -----------------------------
# Flatten / Normalize
# -----------------------------

OUT_COLUMNS = [
    "alert_id", "type_of_alert", "status", "assigned_to_name",
    "creation_date", "resolution_date", "impact_level", "sla_hours",
    "jurisdiction", "category", "tax_code", "monetary_exposure",
    "has_invoice_linked", "order_id", "invoice_id",
]

def to_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except Exception:
        return None

def normalize_alert(payload: Dict[str, Any]) -> Dict[str, Any]:
    # por quê: schema drift é comum; padronizar garante EDA/CSV consistentes
    name = None
    assigned = payload.get("assigned_to")
    if isinstance(assigned, dict):
        name = assigned.get("name")
    return {
        "alert_id": payload.get("alert_id"),
        "type_of_alert": payload.get("type_of_alert"),
        "status": payload.get("status"),
        "assigned_to_name": name,
        "creation_date": payload.get("creation_date"),
        "resolution_date": payload.get("resolution_date"),
        "impact_level": payload.get("impact_level"),
        "sla_hours": payload.get("sla_hours"),
        "jurisdiction": payload.get("jurisdiction"),
        "category": payload.get("category"),
        "tax_code": payload.get("tax_code"),
        "monetary_exposure": to_number(payload.get("monetary_exposure")),
        "has_invoice_linked": payload.get("has_invoice_linked"),
        "order_id": payload.get("order_id"),
        "invoice_id": payload.get("invoice_id"),
    }


# -----------------------------
# Analysis (EDA)
# -----------------------------

def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    k = (len(xs) - 1) * (p / 100.0)
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return float(xs[int(k)])
    return float(xs[f] * (c - k) + xs[c] * (k - f))

def eda_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    status_dist = Counter(r.get("status") for r in rows)
    type_dist = Counter(r.get("type_of_alert") for r in rows)
    impact_dist = Counter(r.get("impact_level") for r in rows)
    with_owner = sum(1 for r in rows if r.get("assigned_to_name"))
    unresolved = sum(1 for r in rows if not r.get("resolution_date"))
    exposures = [r["monetary_exposure"] for r in rows if isinstance(r.get("monetary_exposure"), (int, float))]
    return {
        "total_alerts": len(rows),
        "status_distribution": dict(status_dist),
        "type_distribution": dict(type_dist),
        "impact_distribution": dict(impact_dist),
        "assigned_present": with_owner,
        "assigned_missing": len(rows) - with_owner,
        "unresolved_count": unresolved,
        "monetary_exposure_mean": round(sum(exposures) / len(exposures), 2) if exposures else 0.0,
        "monetary_exposure_p95": percentile(exposures, 95.0) if exposures else 0.0,
    }

def print_summary(summary: Dict[str, Any]) -> None:
    def fmt(d: Dict[str, Any]) -> str:
        return ", ".join(f"{k}:{v}" for k, v in sorted(d.items(), key=lambda kv: (-kv[1], str(kv[0]))))
    print("\n=== Compliance Alerts — EDA (Resumo) ===")
    print(f"Total: {summary['total_alerts']}")
    print(f"Status: {fmt(summary['status_distribution'])}")
    print(f"Tipos: {fmt(summary['type_distribution'])}")
    print(f"Impacto: {fmt(summary['impact_distribution'])}")
    print(f"Atribuídos: {summary['assigned_present']} | Sem dono: {summary['assigned_missing']}")
    print(f"Sem resolução: {summary['unresolved_count']}")
    print(f"Exposure mean: {summary['monetary_exposure_mean']:.2f} | p95: {summary['monetary_exposure_p95']:.2f}\n")


# -----------------------------
# Persistence
# -----------------------------

def write_csv(path: str, rows: List[Dict[str, Any]], header: List[str]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in header})

def write_summary_csv(path: str, summary: Dict[str, Any]) -> None:
    flat = {
        "total_alerts": summary["total_alerts"],
        "assigned_present": summary["assigned_present"],
        "assigned_missing": summary["assigned_missing"],
        "unresolved_count": summary["unresolved_count"],
        "monetary_exposure_mean": summary["monetary_exposure_mean"],
        "monetary_exposure_p95": summary["monetary_exposure_p95"],
    }
    for prefix, dist in [("status", summary["status_distribution"]),
                         ("type", summary["type_distribution"]),
                         ("impact", summary["impact_distribution"])]:
        for k, v in dist.items():
            flat[f"{prefix}_{k}"] = v
    write_csv(path, [flat], list(flat.keys()))


# -----------------------------
# Orchestrator
# -----------------------------

def run(settings: Settings) -> int:
    print(f"[INFO] simulate={settings.simulate} base_url={settings.base_url} limit={settings.limit} page_size={settings.page_size}")
    repo = ComplianceRepository(settings)

    # paginação de IDs (≥100)
    collected: List[str] = []
    offset = 0
    while len(collected) < settings.limit:
        page_ids = repo.list_ids(status="open", limit=settings.page_size, offset=offset)
        if not page_ids:
            break
        collected.extend(page_ids)
        offset += settings.page_size
        sleep_for_rate_limit(settings.rate_limit_per_sec)
    alert_ids = collected[: settings.limit]
    if len(alert_ids) < 100:
        raise RuntimeError(f"Precisa de >=100 IDs; obtidos {len(alert_ids)}")

    # detalhes por ID
    details: List[Dict[str, Any]] = []
    for i, aid in enumerate(alert_ids, 1):
        try:
            d = repo.get_detail(aid)
            if d:
                details.append(d)
        except Exception as e:
            print(f"[WARN] failed alert_id={aid}: {e}", file=sys.stderr)
        if i % 25 == 0:
            print(f"[INFO] progress: {i}/{len(alert_ids)}")
        sleep_for_rate_limit(settings.rate_limit_per_sec)

    # desnormalização
    flat_rows = [normalize_alert(x) for x in details]

    # persistência
    tag = iso_now_date()
    alerts_csv = os.path.join(settings.out_dir, f"compliance_alerts_{tag}.csv")
    write_csv(alerts_csv, flat_rows, OUT_COLUMNS)

    # EDA
    summary = eda_summary(flat_rows)
    print_summary(summary)
    summary_csv = os.path.join(settings.out_dir, f"compliance_summary_{tag}.csv")
    write_summary_csv(summary_csv, summary)

    print(f"[OK] wrote {alerts_csv}")
    print(f"[OK] wrote {summary_csv}")
    return 0


# -----------------------------
# Self-test (E2E opcional)
# -----------------------------

def self_test(out_dir: str) -> None:
    """E2E: simulado, gera CSV e valida esquema/volume."""
    settings = Settings(
        base_url="https://api.mercadolibre.com",
        simulate=True,
        limit=120,
        page_size=40,
        out_dir=out_dir,
        seed=123,
    )
    ensure_dir(out_dir)
    rc = run(settings)
    assert rc == 0, "run() falhou"
    files = [f for f in os.listdir(out_dir) if f.startswith("compliance_alerts_") and f.endswith(".csv")]
    assert files, "CSV principal não encontrado"
    latest = sorted(files)[-1]
    path = os.path.join(out_dir, latest)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) >= 100, "Menos de 100 linhas no CSV"
        for col in OUT_COLUMNS:
            assert col in reader.fieldnames, f"Coluna ausente: {col}"
    print("[SELF-TEST PASSED] CSV com >=100 linhas e schema esperado.")


# -----------------------------
# CLI
# -----------------------------

def parse_args(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description="Extract & denormalize compliance alerts (simulated or real API).")
    p.add_argument("--base-url", type=str, default="https://api.mercadolibre.com")
    p.add_argument("--simulate", action="store_true", default=True, help="Usa API simulada determinística (default: on).")
    p.add_argument("--no-simulate", action="store_false", dest="simulate")
    p.add_argument("--limit", type=int, default=150, help="Quantidade de alertas (>=100).")
    p.add_argument("--page-size", type=int, default=50, help="Tamanho da página de varredura.")
    p.add_argument("--out-dir", type=str, default="data", help="Diretório de saída (CSVs).")
    p.add_argument("--seed", type=int, default=42, help="Seed p/ determinismo.")
    p.add_argument("--self-test", action="store_true", help="Roda teste E2E com asserts.")
    args = p.parse_args(argv)
    return args

def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test(args.out_dir)
        return 0
    settings = Settings(
        base_url=args.base_url,
        simulate=bool(args.simulate),
        limit=int(args.limit),
        page_size=int(args.page_size),
        out_dir=str(args.out_dir),
        seed=int(args.seed),
    )
    ensure_dir(settings.out_dir)
    return run(settings)

if __name__ == "__main__":
    sys.exit(main())
