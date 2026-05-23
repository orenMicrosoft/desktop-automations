"""Command-line interface for the CEF Screener.

Subcommands
-----------
- ``serve``                       — Launch the Flask dashboard (delegates to web.main)
- ``buy``                         — Print scored gatekeeper rows as a table
- ``sell``                        — Print holdings + sell signals
- ``inspect TICKER``              — Print one ticker's detailed score breakdown
- ``refresh [--tickers T1 T2]``   — Pull from CEFConnect and refresh cache
- ``position add TKR SHARES COST DATE``  — Add a position to positions.json
- ``position remove TKR``         — Remove a position by ticker
- ``position list``               — List tracked positions
- ``diagnose [TICKER]``           — Probe CEFConnect endpoints
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import engine, ingest, portfolio


def _print_table(rows: list[dict], cols: list[str]) -> None:
    if not rows:
        print("(no rows)")
        return
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = "  ".join(c.ljust(widths[c]) for c in cols)
    sep = "  ".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in rows:
        print("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def _fmt(v: Any, digits: int = 2) -> str:
    if v is None:
        return "—"
    try:
        f = float(v)
        if f != f:    # NaN
            return "—"
        return f"{f:.{digits}f}"
    except (TypeError, ValueError):
        return str(v)


# ---------------------------------------------------------------- subcommand handlers
def _cmd_serve(args: argparse.Namespace) -> int:
    from . import web    # local import keeps `cli buy` snappy
    web_args = ["--host", args.host, "--port", str(args.port)]
    if args.no_browser:
        web_args.append("--no-browser")
    return web.main(web_args)


def _cmd_buy(_args: argparse.Namespace) -> int:
    result = engine.run_pipeline()
    if result.scored.empty:
        print("Universe empty or no scored rows. Run `cef-screen refresh` first.")
        return 1
    rows = []
    for _, r in result.scored.iterrows():
        rows.append({
            "ticker": r["ticker"],
            "disc%": _fmt(r.get("current_discount_pct")),
            "z1": _fmt(r.get("z1")),
            "comp": _fmt(r.get("composite"), 1),
            "trap": r.get("trap_tier", "—"),
            "buy": r.get("buy_label", "—"),
        })
    print(f"Snapshot {result.snapshot_date} · {result.universe_size} funds "
          f"({result.liquid_universe_size} liquid)")
    for w in result.warnings:
        print(f"  ⚠ {w}")
    _print_table(rows, ["ticker", "disc%", "z1", "comp", "trap", "buy"])
    return 0


def _cmd_sell(_args: argparse.Namespace) -> int:
    result = engine.run_pipeline()
    if not result.holdings:
        print("No holdings tracked. Add with `cef-screen position add ...`")
        return 0
    rows = []
    for h in result.holdings:
        pos = h["position"]
        ret = h["return"]
        sig = h["sell"]
        rows.append({
            "ticker": pos["ticker"],
            "shares": pos["shares"],
            "cost": _fmt(pos["cost_basis"]),
            "total%": _fmt(ret.get("total_pct"), 4),
            "action": sig.get("action", "HOLD"),
            "urg": sig.get("urgency", 0),
            "reason": sig.get("reason", "—"),
        })
    _print_table(rows, ["ticker", "shares", "cost", "total%",
                        "action", "urg", "reason"])
    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    result = engine.run_pipeline()
    df = result.scored
    if df.empty or "ticker" not in df.columns:
        print("No scored data — run refresh first.")
        return 1
    match = df[df["ticker"].str.upper() == args.ticker.upper()]
    if match.empty:
        print(f"Ticker {args.ticker} not found in scored set.")
        return 1
    r = match.iloc[0].to_dict()
    keys = [
        ("Name", r.get("name", "—")),
        ("Category", r.get("category_name", "—")),
        ("Discount %", _fmt(r.get("current_discount_pct"))),
        ("Median 5Y disc %", _fmt(r.get("median_disc_5y"))),
        ("Z 1Y", _fmt(r.get("z1"))),
        ("NAV CAGR 3Y", _fmt(r.get("nav_cagr_3y"), 4)),
        ("NAV TR 3Y", _fmt(r.get("nav_total_return_3y"), 4)),
        ("Coverage", _fmt(r.get("coverage"), 3)),
        ("Composite", _fmt(r.get("composite"), 1)),
        ("Multiplier", _fmt(r.get("multiplier"), 3)),
        ("S Disc", _fmt(r.get("s_disc"), 1)),
        ("S Res", _fmt(r.get("s_res"), 1)),
        ("S Sust", _fmt(r.get("s_sust"), 1)),
        ("S Peer", _fmt(r.get("s_peer"), 1)),
        ("Trap tier", r.get("trap_tier", "—")),
        ("Trap reason", r.get("trap_reason") or "—"),
        ("Buy label", r.get("buy_label", "—")),
    ]
    for k, v in keys:
        print(f"  {k:20s} {v}")
    return 0


def _cmd_refresh(args: argparse.Namespace) -> int:
    tickers = args.tickers if args.tickers else None
    summary = engine.refresh_universe(tickers=tickers)
    print("Refresh complete:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    return 0


def _cmd_position(args: argparse.Namespace) -> int:    # noqa: C901
    action = args.action
    if action == "add":
        portfolio.add_position(args.ticker, args.shares, args.cost,
                               args.date)
        print(f"Added position: {args.ticker}")
        return 0
    if action == "remove":
        remaining = portfolio.remove_position(args.ticker)
        print(f"Removed {args.ticker}. {len(remaining)} positions remain.")
        return 0
    if action == "list":
        positions = portfolio.load_positions()
        if not positions:
            print("(no positions)")
            return 0
        for p in positions:
            print(f"  {p.ticker:6s} {p.shares:>10g}  @ {p.cost_basis:>8.2f}  "
                  f"({p.purchase_date})")
        return 0
    print(f"Unknown position action: {action}")    # pragma: no cover
    return 2    # pragma: no cover


def _cmd_diagnose(args: argparse.Namespace) -> int:
    report = ingest.diagnose(args.ticker)
    print(json.dumps(report, indent=2, default=str))
    return 0 if all(report.values()) else 1


# ---------------------------------------------------------------- parser
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cef-screen",
                                description="CEF Quantitative Screener CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    serve = sub.add_parser("serve", help="Launch the Flask dashboard")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8100)
    serve.add_argument("--no-browser", action="store_true")
    serve.set_defaults(func=_cmd_serve)

    buy = sub.add_parser("buy", help="Print scored buy candidates")
    buy.set_defaults(func=_cmd_buy)

    sell = sub.add_parser("sell", help="Print holdings + sell signals")
    sell.set_defaults(func=_cmd_sell)

    inspect = sub.add_parser("inspect", help="Drill into one ticker")
    inspect.add_argument("ticker")
    inspect.set_defaults(func=_cmd_inspect)

    refresh = sub.add_parser("refresh", help="Fetch from CEFConnect and update cache")
    refresh.add_argument("--tickers", nargs="*",
                         help="Also refresh per-ticker histories")
    refresh.set_defaults(func=_cmd_refresh)

    pos = sub.add_parser("position", help="Manage positions.json")
    pos_sub = pos.add_subparsers(dest="action", required=True)
    pos_add = pos_sub.add_parser("add")
    pos_add.add_argument("ticker")
    pos_add.add_argument("shares", type=float)
    pos_add.add_argument("cost", type=float)
    pos_add.add_argument("date")
    pos_rm = pos_sub.add_parser("remove")
    pos_rm.add_argument("ticker")
    pos_sub.add_parser("list")
    pos.set_defaults(func=_cmd_position)

    diag = sub.add_parser("diagnose", help="Probe CEFConnect endpoints")
    diag.add_argument("ticker", nargs="?", default="PFL")
    diag.set_defaults(func=_cmd_diagnose)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    return args.func(args)


if __name__ == "__main__":    # pragma: no cover
    raise SystemExit(main())
