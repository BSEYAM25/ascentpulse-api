from fastapi import FastAPI, Header, HTTPException
from typing import Optional
import os

app = FastAPI(title="AscentPulse API")

# --- simple auth for cron/job endpoints ---
def check_key(auth: Optional[str]):
    expected = os.getenv("API_KEY")
    if expected and auth != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- health & root ---
@app.get("/")
def root():
    return {"ok": True, "service": "ascentpulse-api"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# --- READ endpoints for UI ---
@app.get("/regime/today")
def regime_today():
    return {"date": "today", "regime": "RISK_ON", "confidence": 0.62}

@app.get("/scores/top10")
def scores_top10():
    return {
        "asof": "today",
        "items": [
            {"symbol": "SOLUSDT", "score": 78.4, "why": ["RS strong", "positive basis"]},
            {"symbol": "INJUSDT", "score": 75.1, "why": ["OBV ↑", "VWAP reclaim"]},
        ],
    }

@app.get("/signals/recent")
def signals_recent(limit: int = 50):
    return {"items": []}

# --- JOB endpoints for cron (protected) ---
@app.post("/jobs/hygiene")
def job_hygiene(authorization: Optional[str] = Header(None)):
    check_key(authorization); return {"ok": True, "job": "hygiene"}

@app.post("/jobs/score")
def job_score(authorization: Optional[str] = Header(None)):
    check_key(authorization); return {"ok": True, "job": "score"}

@app.post("/jobs/trigger4h")
def job_trigger4h(authorization: Optional[str] = Header(None)):
    check_key(authorization); return {"ok": True, "job": "trigger4h"}

# --- test alert ---
@app.post("/alerts/test")
def alerts_test(authorization: Optional[str] = Header(None)):
    check_key(authorization); return {"ok": True, "sent": True}
