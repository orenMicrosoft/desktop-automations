"""Holdings persistence (positions.json) + sell-trigger evaluation.

A position is a dict ``{ticker, shares, cost_basis, purchase_date}``.
Total-return calc: price gain + sum of distributions since purchase date.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from . import config, rules


@dataclass
class Position:
    ticker: str
    shares: float
    cost_basis: float
    purchase_date: str          # ISO yyyy-mm-dd

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "shares": float(self.shares),
            "cost_basis": float(self.cost_basis),
            "purchase_date": self.purchase_date,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        return cls(
            ticker=str(d["ticker"]).upper(),
            shares=float(d["shares"]),
            cost_basis=float(d["cost_basis"]),
            purchase_date=str(d["purchase_date"]),
        )


def load_positions(path: Path | None = None) -> list[Position]:
    """Read positions from ``positions.json``; empty file ↔ empty list."""
    p = Path(path) if path else config.positions_path()
    if not p.exists():
        return []
    raw = p.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("positions.json must be a JSON list of position dicts")
    return [Position.from_dict(d) for d in data]


def save_positions(positions: Iterable[Position], path: Path | None = None) -> None:
    p = Path(path) if path else config.positions_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps([pos.to_dict() for pos in positions], indent=2),
        encoding="utf-8",
    )


def add_position(
    ticker: str, shares: float, cost_basis: float,
    purchase_date: str | None = None, *, path: Path | None = None,
) -> list[Position]:
    """Upsert a position by ticker; persist and return updated list."""
    purchase_date = purchase_date or date.today().isoformat()
    positions = [p for p in load_positions(path) if p.ticker.upper() != ticker.upper()]
    positions.append(Position(
        ticker=ticker.upper(),
        shares=float(shares),
        cost_basis=float(cost_basis),
        purchase_date=purchase_date,
    ))
    save_positions(positions, path)
    return positions


def remove_position(ticker: str, *, path: Path | None = None) -> list[Position]:
    positions = [p for p in load_positions(path) if p.ticker.upper() != ticker.upper()]
    save_positions(positions, path)
    return positions


# ---------------------------------------------------------------- return calc
def distributions_since(
    distribution_rows: Sequence[dict], purchase_date: str,
) -> float:
    """Sum of ``tot_div`` for ex/declared dates strictly after ``purchase_date``."""
    cutoff = _parse_iso(purchase_date)
    if cutoff is None:
        return 0.0
    total = 0.0
    for row in distribution_rows:
        ex_or_declared = (row.get("ex_date") or row.get("declared_date"))
        d = _parse_iso(ex_or_declared)
        if d is None or d <= cutoff:
            continue
        amt = row.get("tot_div")
        if amt is None:
            continue
        try:
            total += float(amt)
        except (TypeError, ValueError):
            continue
    return total


def position_return(
    position: Position,
    current_price: float | None,
    distribution_rows: Sequence[dict] | None,
) -> dict:
    """Compute price return, distribution income, and total return fractions."""
    if current_price is None or position.cost_basis <= 0:
        return {"price_pct": None, "dist_pct": None, "total_pct": None}
    price_pct = (current_price - position.cost_basis) / position.cost_basis
    dist_total = distributions_since(distribution_rows or [], position.purchase_date)
    dist_pct = dist_total / position.cost_basis
    total_pct = price_pct + dist_pct
    return {
        "price_pct": price_pct,
        "dist_pct": dist_pct,
        "total_pct": total_pct,
        "dist_dollars_per_share": dist_total,
    }


def evaluate_position(
    position: Position,
    *,
    current_price: float | None,
    z1: float | None,
    z3: float | None,
    distribution_rows: Sequence[dict] | None = None,
) -> dict:
    """Combine return calc with sell-trigger evaluation."""
    ret = position_return(position, current_price, distribution_rows)
    triggers = rules.evaluate_sell_triggers(
        z1=z1, z3=z3, return_pct=ret["total_pct"],
    )
    return {
        "position": position.to_dict(),
        "current_price": current_price,
        "z1": z1,
        "z3": z3,
        "return": ret,
        "triggers": triggers["triggers"],
        "urgency": triggers["urgency"],
    }


def evaluate_portfolio(
    positions: Iterable[Position],
    snapshot_by_ticker: dict,
    distributions_by_ticker: dict | None = None,
) -> list[dict]:
    """Evaluate all positions; return alerts sorted by urgency desc."""
    distributions_by_ticker = distributions_by_ticker or {}
    out: list[dict] = []
    for pos in positions:
        snap = snapshot_by_ticker.get(pos.ticker.upper()) or {}
        out.append(evaluate_position(
            pos,
            current_price=snap.get("price"),
            z1=snap.get("z1"),
            z3=snap.get("z3"),
            distribution_rows=distributions_by_ticker.get(pos.ticker.upper(), []),
        ))
    out.sort(key=lambda r: r["urgency"], reverse=True)
    return out


def _parse_iso(s) -> date | None:
    if s is None:
        return None
    try:
        if pd.isna(s):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    try:
        return datetime.fromisoformat(str(s)[:10]).date()
    except (TypeError, ValueError):
        return None
