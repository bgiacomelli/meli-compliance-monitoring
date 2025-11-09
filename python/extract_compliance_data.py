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
    backoff_factor: float = 0.5
    rate_limit_per_sec: float = 5.0


def iso_now_date() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sleep_for_rate_limit(rate_per_sec: float) -> None:
    if rate_per_sec <= 0:
        return
    time.sleep(1.0 / rate_per_sec)


class HttpClient:
    def __init__(self, base_url: str, timeout_sec: int, max_retries: int, backoff_factor: float):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
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
            payload = {"_raw_text": resp.text}
        return resp.status_code, payload


class SimulatedComplianceAPI:
    TYPES = ["MISSING_INVOICE", "WRONG_TAX_RATE", "INVOICE_AMOUNT_MISMATCH", "TAX_JURISDICTION_ERROR"]
    STATUSES = ["open", "in_progress", "closed"]
    IMPACT = ["low", "medium", "high", "critical"]
    CATEGORIES = ["Electronics", "Books", "Home", "Games", "Beauty"]
    TAX_CODES = ["ICMS", "IPI", "PIS", "COFINS", "ISS"]

    def __init__(self, seed: int):
        self.seed = seed
        self.rng = random.Random(seed)

    def list_alert_ids(self, status: str, limit: int, offset: int) -> Dict[str, Any]:
        base_rng = random.Random(hash((self.seed, status)) & 0xFFFFFFFF)
        total = max(limit + offset, 200)
        ids = []
        for i in range(offset, min(offset + limit, total)):
            ids.append(f"ALRT-{base_rng.randint(10_000, 99_999)}-{i}")
        return {"status": status, "count": len(ids), "total": total, "data": ids}

    def get_alert_detail(self, alert_id: str) -> Dict[str, Any]:
        h = abs(hash((self.seed, alert_id)))
        rng = random.Random(h)

        created = dt.datetime.now(dt.timezone.utc) - dt.timedelta(
            days=rng.randint(0, 120),
            hours=rng.randint(0, 23),
        )

        sla_hours = rng.choice([24, 48, 72, 168])
        is_closed = rng.random() < 0.60

        if is_closed:
            u = rng.random()
            if u < 0.70:
                max_days = max(1, int(sla_hours / 24 * 0.9))
                days_to_resolve = rng.randint(1, max_days)
            elif u < 0.85:
                base = max(1, int(sla_hours / 24))
                days_to_resolve = base + rng.randint(0, 2)
            else:
                base = max(1, int(sla_hours / 24))
                days_to_resolve = base + rng.randint(3, 10)

            resolution_date = created + dt.timedelta(days=days_to_resolve)
            now_utc = dt.datetime.now(dt.timezone.utc)
            if resolution_date > now_utc:
                resolution_date = now_utc
        else:
            resolution_date = None

        assigned_to = None if rng.random() < 0.10 else {
            "id": f"USR-{rng.randint(1000, 9999)}",
            "name": rng.choice(["Ana", "Bruno", "Carla", "Diego", "Eva", "Felipe"]),
        }

        type_of_alert = rng.choice(self.TYPES)
        impact = rng.choices(self.IMPACT, weights=[4, 3, 2, 1])[0]
        status = "closed" if is_closed else rng.choice(["open", "in_progress"])
        category = rng.choice(self.CATEGORIES)
        tax_code = rng.choice(self.TAX_CODES)
        jurisdiction = rng.choice(["BR-SP", "BR-RJ", "BR-MG", "BR-RS", "BR-PR"])

        monetary_exposure = round(rng.uniform(0, 50_000), 2)
        if rng.random() < 0.05:
            monetary_exposure = str(monetary_exposure)

        has_invoice_linked = rng.random() < 0.7
        order_id = None if rng.random() < 0.2 else f"O{rng.randint(10_000, 99_999)}"
        invoice_id = None if not has_invoice_linked or rng.random() < 0.15 else f"INV-{rng.randint(10_000, 99_999)}"

        return {
            "alert_id": alert_id,
            "type_of_alert": type_of_alert,
            "status": status,
            "assigned_to": assigned_to,
            "creation_date": created.isoformat(),
            "resolution_date": resolution_date.isoformat() if resolution_date else None,
            "impact_level": impact,
            "sla_hours": sla_hours,
            "jurisdiction": jurisdiction,
            "category": category,
            "tax_code": tax_code,
            "monetary_exposure": monetary_exposure,
            "has_invoice_linked": has_invoice_linked,
            "order_id": order_id,
            "invoice_id": invoice_id,
        }


class ComplianceRepository:
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
        data = payload.get("data", [])
        return [str(x) for x in data]

    def get_detail(self, alert_id: str) -> Dict[str, Any]:
        attempts = 0
        last_err: Optional[Exception] = None
        while attempts < (self.settings.max_retries + 1):
            try:
                if self.sim:
                    return self.sim.get_alert_detail(alert_id)
                else:
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


OUT_COLUMNS = [
    "alert_id",
    "type_of_alert",
    "status",
    "assigned_to_name",
    "creation_date",
    "resolution_date",
    "impact_level",
    "sla_hours",
    "jurisdiction",
    "category",
    "tax_code",
    "monetary_exposure",
    "has_invoice_linked",
    "order_id",
    "invoice_id",
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
    assigned_name = None
    assigned = payload.get("assigned_to")
    if isinstance(assigned, dict):
        assigned_name = assigned.get("name")

    return {
        "alert_id": payload.get("alert_id"),
        "type_of_alert": payload.get("type_of_alert"),
        "status": payload.get("status"),
        "assigned_to_name": assigned_name,
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


def eda_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    status_dist = Counter(r.get("status") for r in rows)
    type_dist = Counter(r.get("type_of_alert") for r in rows)
    impact_dist = Counter(r.get("impact_level") for r in rows)
    with_owner = sum(1 for r in rows if r.get("assigned_to_name"))
    missing_owner = len(rows) - with_owner
    unresolved = sum(1 for r in rows if not r.get("resolution_date"))
    exposures = [r["monetary_exposure"] for r in rows if isinstance(r.get("monetary_exposure"), (int, float))]
    exp_mean = round(sum(exposures) / len(exposures), 2) if exposures else 0.0
    exp_p95 = percentile(exposures, 95.0) if exposures else 0.0

    return {
        "total_alerts": len(rows),
        "status_distribution": dict(status_dist),
        "type_distribution": dict(type_dist),
        "impact_distribution": dict(impact_dist),
        "assigned_missing": missing_owner,
        "assigned_present": with_owner,
        "unresolved_count": unresolved,
        "monetary_exposure_mean": exp_mean,
        "monetary_exposure_p95": exp_p95,
    }


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    k = (len(xs) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(xs[int(k)])
    d0 = xs[f] * (c - k)
    d1 = xs[c] * (k - f)
    return float(d0 + d1)


def print_summary(summary: Dict[str, Any]) -> None:
    def fmt_counter(d: Dict[str, Any]) -> str:
        return ", ".join(f"{k}:{v}" for k, v in sorted(d.items(), key=lambda kv: (-kv[1], str(kv[0]))))
    print("\n=== Compliance Alerts — EDA (Resumo) ===")
    print(f"Total alerts: {summary['total_alerts']}")
    print(f"Status: {fmt_counter(summary['status_distribution'])}")
    print(f"Tipos: {fmt_counter(summary['type_distribution'])}")
    print(f"Impacto: {fmt_counter(summary['impact_distribution'])}")
    print(f"Atribuídos: {summary['assigned_present']} | Sem dono: {summary['assigned_missing']}")
    print(f"Sem resolução (abertos+andamento): {summary['unresolved_count']}")
    print(f"Monetary exposure mean: {summary['monetary_exposure_mean']:.2f} | p95: {summary['monetary_exposure_p95']:.2f}\n")


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
    for prefix, dist in [
        ("status", summary["status_distribution"]),
        ("type", summary["type_distribution"]),
        ("impact", summary["impact_distribution"]),
    ]:
        for k, v in dist.items():
            flat[f"{prefix}_{k}"] = v
    write_csv(path, [flat], list(flat.keys()))


def run(settings: Settings) -> int:
    print(f"[INFO] simulate={settings.simulate} base_url={settings.base_url} limit={settings.limit} page_size={settings.page_size}")
    repo = ComplianceRepository(settings)

    collected: List[str] = []
    offset = 0
    while len(collected) < settings.limit:
        page = repo.list_ids(status="open", limit=settings.page_size, offset=offset)
        if not page:
            break
        collected.extend(page)
        offset += settings.page_size
        sleep_for_rate_limit(settings.rate_limit_per_sec)
    alert_ids = collected[: settings.limit]
    print(f"[INFO] fetched {len(alert_ids)} alert ids")

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

    flat_rows = [normalize_alert(x) for x in details]

    ensure_dir(settings.out_dir)
    date_tag = iso_now_date()
    out_path = os.path.join(settings.out_dir, f"compliance_alerts_{date_tag}.csv")
    write_csv(out_path, flat_rows, OUT_COLUMNS)

    summary = eda_summary(flat_rows)
    print_summary(summary)
    summary_path = os.path.join(settings.out_dir, f"compliance_summary_{date_tag}.csv")
    write_summary_csv(summary_path, summary)

    print(f"[OK] wrote {out_path}")
    print(f"[OK] wrote {summary_path}")
    return 0


def parse_args(argv: Optional[List[str]] = None) -> Settings:
    parser = argparse.ArgumentParser(description="Extract & denormalize compliance alerts from API (simulated or real).")
    parser.add_argument("--base-url", type=str, default="https://api.mercadolibre.com",
                        help="Base URL da API real (quando --simulate=off).")
    parser.add_argument("--simulate", action="store_true", default=True,
                        help="Usar API simulada determinística (default: on). Passe --no-simulate para desativar.", )
    parser.add_argument("--no-simulate", action="store_false", dest="simulate")
    parser.add_argument("--limit", type=int, default=150, help="Quantidade de alertas a coletar (>=100).")
    parser.add_argument("--page-size", type=int, default=50, help="Tamanho de página para paginação.")
    parser.add_argument("--out-dir", type=str, default="data", help="Diretório de saída para CSVs.")
    parser.add_argument("--seed", type=int, default=42, help="Seed p/ gerador determinístico.")
    args = parser.parse_args(argv)

    return Settings(
        base_url=args.base_url,
        simulate=bool(args.simulate),
        limit=int(args.limit),
        page_size=int(args.page_size),
        out_dir=str(args.out_dir),
        seed=int(args.seed),
    )


if __name__ == "__main__":
    sys.exit(run(parse_args()))
