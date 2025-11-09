from fastapi import FastAPI, Header, HTTPException
from typing import Optional, List
import os
import datetime as dt
import psycopg
from psycopg.types.json import Json

app = FastAPI(title="AscentPulse API")

# ------------------ Auth helper ------------------
def check_key(auth: Optional[str]):
    """Protects /jobs/* endpoints with a static bearer key."""
    expected = os.getenv("API_KEY")
    if expected is None:
        return  # no auth if key not set (not recommended)
    if auth != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="Unauthorized")

# ------------------ DB helper ------------------
DB_URL = os.getenv("DATABASE_URL")

def db_conn():
    if not DB_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")
    return psycopg.connect(DB_URL, autocommit=True)

# ------------------ Root / Health ------------------
@app.get("/")
def root():
    return {"ok": True, "service": "ascentpulse-api"}

@app.get("/health")
def health():
    db_ok = False
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("select 1")
            cur.fetchone()
        db_ok = True
    except Exception:
        db_ok = False
    return {"status": "healthy", "db": db_ok}

# ------------------ READ endpoints for UI ------------------
@app.get("/regime/today")
def regime_today():
    """
    Returns latest regime snapshot from market_state_daily,
    falls back to a stub if table empty.
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            select asof, regime, confidence
            from market_state_daily
            order by asof desc
            limit 1
        """)
        row = cur.fetchone()
    if row:
        asof, regime, conf = row
        return {"date": str(asof), "regime": regime, "confidence": float(conf) if conf is not None else None}
    # fallback if empty
    return {"date": "today", "regime": "RISK_ON", "confidence": 0.62}

@app.get("/scores/top10")
def scores_top10():
    """
    Returns top-10 by latest asof from score_daily.
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            with latest as (select max(asof) as asof from score_daily)
            select s.asof, s.symbol, s.score, s.rank, s.reasons
            from score_daily s
            join latest l on s.asof = l.asof
            order by s.rank asc nulls last, s.score desc
            limit 10
        """)
        rows = cur.fetchall()
    if not rows:
        # fallback demo if empty
        return {
            "asof": "today",
            "items": [
                {"symbol": "SOLUSDT", "score": 78.4, "why": ["RS strong", "positive basis"]},
                {"symbol": "INJUSDT", "score": 75.1, "why": ["OBV ↑", "VWAP reclaim"]},
            ],
        }
    items = []
    latest_asof = None
    for asof, sym, score, rnk, reasons in rows:
        latest_asof = asof
        items.append({
            "symbol": sym,
            "score": float(score) if score is not None else None,
            "rank": rnk,
            "why": reasons if reasons else []
        })
    return {"asof": str(latest_asof), "items": items}

@app.get("/signals/recent")
def signals_recent(limit: int = 50):
    """
    Returns recent 4H signals from signal_4h.
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            select ts, symbol, trigger, entry, stop, tp1, tp2, regime, score, status
            from signal_4h
            order by ts desc nulls last, created_at desc
            limit %s
        """, (limit,))
        rows = cur.fetchall()
    items = []
    for (ts, sym, trig, entry, stop, tp1, tp2, regime, score, status) in rows:
        items.append({
            "ts": ts.isoformat() if ts else None,
            "symbol": sym,
            "trigger": trig,
            "entry": float(entry) if entry is not None else None,
            "stop": float(stop) if stop is not None else None,
            "tp1": float(tp1) if tp1 is not None else None,
            "tp2": float(tp2) if tp2 is not None else None,
            "regime": regime,
            "score": float(score) if score is not None else None,
            "status": status
        })
    return {"items": items}

# ------------------ JOB endpoints (Cron targets) ------------------
@app.post("/jobs/hygiene")
def job_hygiene(authorization: Optional[str] = Header(None)):
    """
    Upserts today's regime snapshot.
    Replace the payload with real calculations later.
    """
    check_key(authorization)
    today = dt.date.today()
    payload = {
        "btc_trend": "UP",
        "eth_trend": "UP",
        "breadth_pct": 62.0,
        "stables_flow_pct": -0.4,
        "regime": "RISK_ON",
        "confidence": 0.63
    }
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            insert into market_state_daily
                (asof, btc_trend, eth_trend, breadth_pct, stables_flow_pct, regime, confidence)
            values (%s,%s,%s,%s,%s,%s,%s)
            on conflict (asof) do update
            set btc_trend=excluded.btc_trend,
                eth_trend=excluded.eth_trend,
                breadth_pct=excluded.breadth_pct,
                stables_flow_pct=excluded.stables_flow_pct,
                regime=excluded.regime,
                confidence=excluded.confidence
        """, (
            today,
            payload["btc_trend"], payload["eth_trend"],
            payload["breadth_pct"], payload["stables_flow_pct"],
            payload["regime"], payload["confidence"]
        ))
    return {"ok": True, "job": "hygiene"}

@app.post("/jobs/score")
def job_score(authorization: Optional[str] = Header(None)):
    """
    Upserts today's top scores (example rows).
    Replace 'top' with your scoring outputs later.
    """
    check_key(authorization)
    today = dt.date.today()
    top: List[tuple] = [
        ("SOLUSDT", 78.4, 1, ["RS strong", "positive basis"]),
        ("INJUSDT", 75.1, 2, ["OBV ↑", "VWAP reclaim"]),
        ("TIAUSDT", 73.2, 3, ["AVWAP reclaim"]),
    ]
    with db_conn() as conn, conn.cursor() as cur:
        for sym, score, rnk, why in top:
            cur.execute("""
                insert into score_daily (asof, symbol, score, reasons, rank)
                values (%s,%s,%s,%s,%s)
                on conflict (asof, symbol) do update
                set score=excluded.score,
                    reasons=excluded.reasons,
                    rank=excluded.rank
            """, (today, sym, score, Json(why), rnk))
    return {"ok": True, "job": "score", "count": len(top)}

@app.post("/jobs/trigger4h")
def job_trigger4h(authorization: Optional[str] = Header(None)):
    """
    Inserts example 4H triggers. Replace with real detection logic later.
    """
    check_key(authorization)
    now = dt.datetime.utcnow().replace(microsecond=0)
    sample = [
        (now, "SOLUSDT", "sweep→CHoCH→retest", 178.20, 169.50, 189.00, 198.00, "RISK_ON", 78.4),
        (now, "INJUSDT",  "AVWAP reclaim",      31.10,  28.90,  34.50,  37.00, "RISK_ON", 75.1),
    ]
    with db_conn() as conn, conn.cursor() as cur:
        for ts, sym, trig, entry, stop, tp1, tp2, regime, score in sample:
            cur.execute("""
                insert into signal_4h
                    (ts, symbol, trigger, entry, stop, tp1, tp2, regime, score)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (ts, sym, trig, entry, stop, tp1, tp2, regime, score))
    return {"ok": True, "job": "trigger4h", "count": len(sample)}

@app.post("/jobs/derivs")
def job_derivs(authorization: Optional[str] = Header(None)):
    """
    Inserts example hourly derivatives snapshot.
    Replace with real OI/funding/basis pulls later.
    """
    check_key(authorization)
    now = dt.datetime.utcnow().replace(minute=10, second=0, microsecond=0)  # align to :10
    rows = [
        (now, "SOLUSDT", 980_000_000, 25_000_000, 0.004, 0.35, 18.0, 22.0, True),
        (now, "INJUSDT", 210_000_000,  6_000_000, 0.002, 0.20,  5.5,  7.2, False),
    ]
    with db_conn() as conn, conn.cursor() as cur:
        for (ts, sym, oi_usd, oi_chg, funding, basis, liq_up, liq_dn, spot_leads) in rows:
            cur.execute("""
                insert into derivs_hourly
                    (ts, symbol, oi_usd, oi_1h_chg, funding_8h, basis_pct, liq_up_m, liq_dn_m, spot_leads)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (ts, sym, oi_usd, oi_chg, funding, basis, liq_up, liq_dn, spot_leads))
    return {"ok": True, "job": "derivs", "count": len(rows)}

# ------------------ Admin / test ------------------
@app.post("/alerts/test")
def alerts_test(authorization: Optional[str] = Header(None)):
    check_key(authorization)
    # Later: push Telegram/email. For now, ack.
    return {"ok": True, "sent": True}
