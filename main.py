# main.py — FastAPI + Google Sheets + Minimal UI (Upload CSV + Generate Teams → opens /teams)
from __future__ import annotations

import base64, csv, io, os, re
from typing import List, Dict, Tuple, Optional
from datetime import datetime, date
from collections import defaultdict

from fastapi import FastAPI, UploadFile, File, Body
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

from algorithm import generate_teams  # <- pure logic module
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
# ----------------------- Config -----------------------
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
GMAIL_QUERY = os.getenv("GMAIL_QUERY", 'has:attachment (filename:csv OR filename:txt) newer_than:14d')

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SESSIONS_TAB = os.getenv("SESSIONS_TAB", "Sessions")
ASSIGNMENTS_TAB = os.getenv("ASSIGNMENTS_TAB", "Assignments")

VALID_POS = {"setter", "middle", "oppo", "outside"}

app = FastAPI(title="Volleyball Teams — CSV → Generate")

STATE = {
    "players": [],    # [{name, gender, pref1, pref2, pref3}]
    "teams": [],      # [{team, players:[{name,pos,gender}]}]
    "source": "",
    "updated_at": None,
    "seed": None,
    "session_id": None,   # last generated session_id (DRAFT)
}

def _touch_state():
    STATE["updated_at"] = datetime.utcnow().isoformat() + "Z"

# ----------------- Google API Helpers -----------------
def credentials():
    """
    Load service account credentials from the JSON key file.
    """
    service_account_file = "service_account.json"  # your key file name
    if not os.path.exists(service_account_file):
        raise RuntimeError("service_account.json not found. Place it in the project folder.")
    creds = ServiceAccountCredentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return creds

def sheets_service():
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Set GOOGLE_SHEET_ID in the environment.")
    return build("sheets", "v4", credentials=credentials(), cache_discovery=False)

# ----------------- CSV Parsing ------------------------
def _normalize_pos(x: str) -> str:
    v = (x or "").strip().lower()
    if v not in VALID_POS:
        return ""
    if v == "oppo":
        return "outside"
    return v

def parse_csv_bytes(b: bytes) -> List[Dict[str, str]]:
    text = b.decode("utf-8", errors="ignore")
    out: List[Dict[str, str]] = []
    try:
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            row_ci = { (k or '').strip().lower(): (v or '').strip() for k,v in row.items() }
            name   = row_ci.get("name") or row_ci.get("player") or row_ci.get("jogador")
            gender = (row_ci.get("gender") or "").lower()
            email  = row_ci.get("email") or row_ci.get("e-mail") or row_ci.get("mail") or ""
            p1 = _normalize_pos(row_ci.get("pref1") or row_ci.get("y-preferred-position") or "")
            p2 = _normalize_pos(row_ci.get("pref2") or "")
            p3 = _normalize_pos(row_ci.get("pref3") or "")
            if not name:
                continue
            if p1 and p1 == p2 == p3:
                p2 = ""
                p3 = ""
            out.append({"name": name, "gender": gender,"email": email,"pref1": p1, "pref2": p2, "pref3": p3})
    except Exception:
        pass

    # headerless fallback
    if not out:
        raw = list(csv.reader(io.StringIO(text)))
        if raw:
            hdr = [c.strip().lower() for c in raw[0]]
            start = 1 if any(h in ('name','player','jogador') for h in hdr) else 0
            for r in raw[start:]:
                if not r: continue
                nm = (r[0] or '').strip()
                if nm:
                    out.append({"name": nm, "gender": "", "pref1": "", "pref2": "", "pref3": ""})
    return out

# ----------------- Sheets I/O -------------------------
def ensure_tabs_and_headers():
    svc = sheets_service()
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    current = {s["properties"]["title"] for s in meta.get("sheets", [])}
    requests = []
    if SESSIONS_TAB not in current:
        requests.append({"addSheet": {"properties": {"title": SESSIONS_TAB}}})
    if ASSIGNMENTS_TAB not in current:
        requests.append({"addSheet": {"properties": {"title": ASSIGNMENTS_TAB}}})
    if not any(s.lower() == "history" for s in current):
        requests.append({"addSheet": {"properties": {"title": "History"}}})
    if requests:
        svc.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body={"requests": requests}).execute()

    # headers
    def header_ok(tab: str, expected: List[str]) -> bool:
        res = svc.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab}!A1:Z1"
        ).execute()
        vals = res.get("values", [])
        if not vals:
            return False
        row = vals[0]
        return row[:len(expected)] == expected

    if not header_ok(SESSIONS_TAB, ["session_id", "date", "status"]):
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SESSIONS_TAB}!A1:C1",
            valueInputOption="RAW",
            body={"values": [["session_id", "date", "status"]]}
        ).execute()

    if not header_ok(ASSIGNMENTS_TAB, ["session_id", "name", "pref1", "assigned_pos", "out_of_pref1"]):
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{ASSIGNMENTS_TAB}!A1:E1",
            valueInputOption="RAW",
            body={"values": [["session_id", "name", "pref1", "assigned_pos", "out_of_pref1"]]}
        ).execute()

    if not header_ok("History", ["session_id", "name", "pref1", "assigned_pos", "out_of_pref1", "archived_at"]):
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="History!A1:F1",
            valueInputOption="RAW",
            body={"values": [["session_id", "name", "pref1", "assigned_pos", "out_of_pref1", "archived_at"]]}
        ).execute()

def _read_sessions() -> List[List[str]]:
    svc = sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"{SESSIONS_TAB}!A2:C"
    ).execute()
    return res.get("values", []) or []

def _read_assignments() -> List[List[str]]:
    svc = sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"{ASSIGNMENTS_TAB}!A2:E"
    ).execute()
    return res.get("values", []) or []

def _append_sessions(rows: List[List[str]]):
    if not rows: return
    svc = sheets_service()
    svc.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{SESSIONS_TAB}!A:C",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

def _append_assignments(rows: List[List[str]]):
    if not rows: return
    svc = sheets_service()
    svc.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{ASSIGNMENTS_TAB}!A:E",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

def _final_sessions_sorted_desc() -> List[Tuple[str, str]]:
    """
    Return list of (session_id, date) for FINAL sessions sorted by date desc (YYYY-MM-DD).
    """
    sessions = _read_sessions()
    finals: List[Tuple[str, str]] = []
    for r in sessions:
        sid = (r[0] if len(r) > 0 else "").strip()
        d   = (r[1] if len(r) > 1 else "").strip()
        st  = (r[2] if len(r) > 2 else "").strip().upper()
        if sid and d and st == "FINAL":
            finals.append((sid, d))
    # sort desc by date
    finals.sort(key=lambda t: t[1], reverse=True)
    return finals

def _last_two_offpref_maps() -> Tuple[Dict[str, int], Dict[str, bool]]:
    """
    Build fairness maps from the last two FINAL sessions:
      - count map: name -> 0..2 (times off-pref across last two finals)
      - any map:   name -> True/False (off-pref at least once across last two finals)
    """
    finals = _final_sessions_sorted_desc()
    last_two_ids = [sid for (sid, _) in finals[:2]]
    if not last_two_ids:
        return {}, {}

    assignments = _read_assignments()
    count_map: Dict[str, int] = defaultdict(int)
    any_map: Dict[str, bool] = defaultdict(bool)

    for r in assignments:
        if len(r) < 5: 
            continue
        sid, name, pref1, assigned_pos, out_flag = (r + ["", "", "", "", ""])[:5]
        if sid not in last_two_ids:
            continue
        name = name.strip()
        pref1 = (pref1 or "").strip().lower()
        assigned_pos = (assigned_pos or "").strip().lower()
        off = (pref1 and assigned_pos and pref1 != assigned_pos)
        if off:
            count_map[name] += 1
            any_map[name] = True
        else:
            any_map.setdefault(name, any_map[name])

    # clamp [0..2]
    for k in list(count_map.keys()):
        count_map[k] = min(2, max(0, count_map[k]))
    return dict(count_map), dict(any_map)

def _append_draft_session_with_assignments(session_id: str, session_date: str, teams: List[Dict], players_by_name: Dict[str, Dict]):
    """
    Store a DRAFT session and its assignments.
    - If same date already exists → clear Assignments before writing new data.
    - If date changed → move previous off-pref players to History before clearing Assignments.
    """
    svc = sheets_service()

    # Read current assignments
    res = svc.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"{ASSIGNMENTS_TAB}!A2:E"
    ).execute()
    existing = res.get("values", []) or []

    # Detect if previous session is same date
    last_date = None
    if existing:
        first_sid = existing[0][0] if existing[0] else ""
        if first_sid and len(first_sid) >= 10:
            last_date = first_sid[:10]  # YYYY-MM-DD from session_id

    # If different day → archive off-pref == "yes" to History
    if last_date and last_date != session_date:
        offpref_rows = [r for r in existing if len(r) >= 5 and r[4].strip().lower() == "yes"]
        if offpref_rows:
            archived_at = datetime.utcnow().isoformat() + "Z"
            body = {
                "values": [r + [archived_at] for r in offpref_rows]
            }
            svc.spreadsheets().values().append(
                spreadsheetId=GOOGLE_SHEET_ID,
                range="History!A:F",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()

    # Clear assignments before writing new ones
    svc.spreadsheets().values().clear(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"{ASSIGNMENTS_TAB}!A2:E"
    ).execute()

    # Append new DRAFT session
    _append_sessions([[session_id, session_date, "DRAFT"]])

    # Build rows for new assignments
    rows = []
    for t in teams:
        for p in t.get("players", []):
            name = p["name"]
            assigned = p["pos"]
            pref1 = (players_by_name.get(name) or {}).get("pref1", "")
            out_flag = "yes" if (pref1 and pref1.lower() != assigned.lower()) else "no"
            rows.append([session_id, name, pref1, assigned, out_flag])

    # Write fresh assignments
    _append_assignments(rows)
    # ------------------------------------------------------------------
    # Retain only the last 6 sessions in Sessions and History
    # ------------------------------------------------------------------
    try:
        # Read all sessions (after we just appended the new DRAFT row)
        sess_res = svc.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SESSIONS_TAB}!A2:C"
        ).execute()
        sessions_all = sess_res.get("values", []) or []

        # Normalize rows: keep only valid [session_id, date, status]
        norm = []
        for r in sessions_all:
            sid = (r[0] if len(r) > 0 else "").strip()
            d   = (r[1] if len(r) > 1 else "").strip()
            st  = (r[2] if len(r) > 2 else "").strip()
            if sid and d:
                norm.append([sid, d, st])

        # Sort by (date, session_id) ascending, then keep the last 6 (most recent)
        norm.sort(key=lambda x: (x[1], x[0]))
        keep = norm[-6:]
        keep_ids = {row[0] for row in keep}

        # If we had more than 6, rewrite Sessions and prune History
        if len(norm) > 6:
            # Rewrite Sessions (preserve header; replace A2:C with the kept 6)
            svc.spreadsheets().values().clear(
                spreadsheetId=GOOGLE_SHEET_ID,
                range=f"{SESSIONS_TAB}!A2:C"
            ).execute()
            if keep:
                svc.spreadsheets().values().update(
                    spreadsheetId=GOOGLE_SHEET_ID,
                    range=f"{SESSIONS_TAB}!A2",
                    valueInputOption="RAW",
                    body={"values": keep}
                ).execute()

            # Now prune History to only rows whose session_id is in keep_ids
            hist_res = svc.spreadsheets().values().get(
                spreadsheetId=GOOGLE_SHEET_ID,
                range="History!A2:F"
            ).execute()
            history_all = hist_res.get("values", []) or []

            history_keep = [r for r in history_all if (r and (r[0] in keep_ids))]
            svc.spreadsheets().values().clear(
                spreadsheetId=GOOGLE_SHEET_ID,
                range="History!A2:F"
            ).execute()
            if history_keep:
                svc.spreadsheets().values().update(
                    spreadsheetId=GOOGLE_SHEET_ID,
                    range="History!A2",
                    valueInputOption="RAW",
                    body={"values": history_keep}
                ).execute()
    except Exception:
        # If anything goes wrong, do not block team generation.
        pass
# ----------------- Endpoints ---------------------------
@app.post("/upload-csv")
def upload_csv(file: UploadFile = File(...)):
    content = file.file.read()
    players = parse_csv_bytes(content)
    players = [p for p in players if p.get("name")]
    if not players:
        return JSONResponse({"ok": False, "error": "No players parsed from CSV."}, status_code=422)
    STATE["players"] = players
    STATE["teams"] = []
    STATE["source"] = f"upload:{file.filename}"
    _touch_state()
    return {"ok": True, "source": STATE["source"], "total_players": len(players), "players": players, "updated_at": STATE["updated_at"]}

@app.post("/generate-teams")
def generate(payload: Optional[dict] = Body(default=None)):
    """
    Generate teams using current players list.
    - Uses only FINAL sessions for fairness.
    - Saves this generation as DRAFT (not counted in fairness until you later mark FINAL).
    - Opens /teams on the client (frontend handles opening in new tab).
    Body (optional): {"seed": 123, "session_date": "YYYY-MM-DD"}
    """
    players = STATE.get("players", [])
    if not players:
        return JSONResponse({"ok": False, "error": "No players loaded. Upload a CSV first."}, status_code=409)

    ensure_tabs_and_headers()

    # Determine date and optional seed
    today = date.today()
    seed = None
    if payload:
        if "seed" in payload:
            try:
                seed = int(payload["seed"])
            except Exception:
                seed = None
        if "session_date" in payload:
            try:
                y, m, d = [int(x) for x in str(payload["session_date"]).split("-")]
                today = date(y, m, d)
            except Exception:
                pass

    # Build fairness maps from the last two FINAL sessions only
    try:
        last_two_count_map, last_two_any_map = _last_two_offpref_maps()
    except Exception:
        last_two_count_map, last_two_any_map = {}, {}

    # Generate
    import random 
    teams = generate_teams(
        players,
        seed=random.randint(1, 99999),
        last_two_offpref_count_by_name=last_two_count_map,
        last_two_any_offpref_by_name=last_two_any_map
    )

    # Save as DRAFT (this generation does not affect fairness until you finalize it)
    session_id = f"{today.isoformat()}-{datetime.utcnow().strftime('%H%M%S')}"
    players_by_name = {p["name"]: p for p in players}
    try:
        _append_draft_session_with_assignments(session_id, today.isoformat(), teams, players_by_name)
    except Exception:
        pass

    STATE["teams"] = teams
    STATE["seed"] = seed
    STATE["session_id"] = session_id
    _touch_state()

    return {"ok": True, "session_id": session_id, "teams": teams, "updated_at": STATE["updated_at"]}

@app.get("/teams", response_class=HTMLResponse)
def show_teams():
    teams = STATE.get("teams", [])
    if not teams:
        return "<p>No teams yet. Upload a CSV and click 'Generate Teams'.</p>"

    # 1) mapear contagem por nome para detectar duplicados
    name_counts = {}
    for t in teams:
        for p in t.get("players", []):
            nm = p.get("name","")
            name_counts[nm] = name_counts.get(nm, 0) + 1
    dup_names = {n for n,c in name_counts.items() if c > 1}

    html = """
<!doctype html>
<html>
<head>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Teams</title>
  <style>
    body { font-family: system-ui, Arial; margin:0; padding:16px; }
    .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:16px; }
    .card { border:1px solid #ddd; border-radius:10px; padding:12px; }
    .title { font-size:18px; margin:0 0 8px 0; }
    li { margin:4px 0; }
    .pos { font-weight:bold; }
    .info { font-size:14px; color:#555; margin-top:4px; }
    .missing { color:red; font-weight:bold; margin-top:4px; }
  </style>
</head>
<body>
  <h2>Teams</h2>
  <div class="grid">
"""
    for t in teams:
        count = len(t.get("players", []))
        missing = t.get("missing")
        html += f'<div class="card"><h3 class="title">Time {t.get("team")}</h3>'
        html += f'<div class="info">Players: {count}</div>'
        if missing:
            html += f'<div class="missing">Missing – {missing}</div>'
        html += "<ul>"
        for p in t.get("players", []):
            name = p.get("name","")
            label = name
            if name in dup_names:
                em = (p.get("email") or "").strip()
                if em:
                    # escapar os sinais < > no HTML
                    label = f'{name} &lt;{em}&gt;'
            html += f"<li>{label} — <span class='pos'>{p.get('pos','')}</span></li>"
        html += "</ul></div>"
    html += "</div></body></html>"
    return html
@app.post("/reset")
def reset_all():
    STATE["players"] = []
    STATE["teams"] = []
    STATE["source"] = ""
    STATE["seed"] = None
    STATE["session_id"] = None
    _touch_state()
    return {"ok": True, **STATE}

# ----------------- Minimal Frontend --------------------
@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html>
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Volleyball Teams</title>
    <style>
      body { font-family: system-ui, Arial; padding:16px; }
      button {
        font-size: 16px;
        padding: 8px 14px;
        margin: 5px;
        border: 1px solid #ccc;
        border-radius: 6px;
        background: #f5f5f5;
        cursor: pointer;
        transition: background 0.3s, transform 0.1s;
      }
      button:hover { background: #e0e0e0; }
      button:active { background: #ccc; transform: scale(0.97); }
      button.loading {
        background: #0078d7;
        color: white;
        cursor: wait;
      }
      button:disabled {
        opacity: 0.6;
        cursor: not-allowed;
      }
      .row { display:flex; flex-wrap:wrap; align-items:center; gap:8px; margin-bottom:8px; }
      @media (prefers-color-scheme: dark) {
        body { background:#111; color:#eee; }
        button { background:#222; color:#eee; border:1px solid #333; }
        button:hover { background:#333; }
        button.loading { background:#0078d7; color:white; }
      }
    </style>
  </head>
  <body>
    <h2>Volleyball Teams</h2>
    <div class="row">
      <form id="csvForm" onsubmit="return uploadCsv(event)" enctype="multipart/form-data">
        <input type="file" id="csvFile" name="file" accept=".csv" />
        <button type="submit" id="btnUpload">Upload CSV</button>
      </form>
      <button id="btnGenerate" onclick="generateTeams()" disabled>Generate Teams</button>
    </div>
    <div id="meta" style="opacity:.7;"></div>

<script>
async function uploadCsv(e){
  e.preventDefault();
  const btn = document.getElementById('btnUpload');
  btn.classList.add('loading');
  btn.disabled = true;
  const f = document.getElementById('csvFile').files[0];
  if(!f){ alert('Select a CSV'); btn.classList.remove('loading'); btn.disabled = false; return false; }
  const fd = new FormData();
  fd.append('file', f);
  const r = await fetch('/upload-csv', {method:'POST', body: fd});
  const d = await r.json();
  if(!d.ok){ alert(d.error || 'Upload error'); btn.classList.remove('loading'); btn.disabled = false; return false; }
  document.getElementById('btnGenerate').disabled = !(d.players && d.players.length);
  document.getElementById('meta').innerText =
    `Source: ${d.source||'-'} • Updated: ${d.updated_at||'-'} • Players: ${d.total_players||'-'}`;
  btn.classList.remove('loading');
  btn.disabled = false;
  return false;
}

async function generateTeams(){
  const btn = document.getElementById('btnGenerate');
  btn.classList.add('loading');
  btn.disabled = true;
  const r = await fetch('/generate-teams', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({})});
  const d = await r.json();
  btn.classList.remove('loading');
  btn.disabled = false;
  if(!d.ok){ alert(d.error || 'Generate error'); return; }
  window.open('/teams','_blank');
}
</script>
  </body>
</html>
"""