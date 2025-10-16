# main.py — FastAPI + Google Sheets + Minimal UI (Upload CSV + Generate Teams → opens /teams)
from __future__ import annotations

import csv, io, os
from typing import List, Dict, Tuple, Optional
from datetime import datetime, date
from collections import defaultdict
import random

from fastapi import FastAPI, UploadFile, File, Body
from fastapi.responses import HTMLResponse, JSONResponse

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

from algorithm import generate_teams  # <- pure logic module

# ----------------------- Config -----------------------
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SESSIONS_TAB = os.getenv("SESSIONS_TAB", "Sessions")
ASSIGNMENTS_TAB = os.getenv("ASSIGNMENTS_TAB", "Assignments")

# No UI changes: manter o mesmo conjunto de inputs/botões
app = FastAPI(title="Volleyball Teams — CSV → Generate")

STATE = {
    "players": [],    # [{name, gender, email, pref1, pref2, pref3}]
    "teams": [],      # [{team, size, missing, extra_player_index, players:[{name,email,gender,pos}]}]
    "source": "",
    "updated_at": None,
    "seed": None,
    "session_id": None,   # last generated session_id (DRAFT)
}

VALID_POS = {"setter", "middle", "oppo", "outside"}

# ----------------- Helpers ---------------------------
def _touch_state():
    STATE["updated_at"] = datetime.utcnow().isoformat() + "Z"

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

# ----------------- Google API ------------------------
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
    # cache_discovery=False evita warnings locais
    return build("sheets", "v4", credentials=credentials(), cache_discovery=False)

# ----------------- CSV Parsing ------------------------
def _normalize_pos(x: str) -> str:
    v = _norm(x)
    if v == "oppo":
        return "outside"
    if v in {"setter", "middle", "outside"}:
        return v
    return ""  # inválido → vaza como vazio

def parse_csv_bytes(b: bytes) -> List[Dict[str, str]]:
    text = b.decode("utf-8", errors="ignore")
    out: List[Dict[str, str]] = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        row_ci = { (k or '').strip().lower(): (v or '').strip() for k,v in row.items() }
        name   = row_ci.get("name") or row_ci.get("player") or row_ci.get("jogador")
        email  = (row_ci.get("email") or row_ci.get("e-mail") or row_ci.get("mail") or "").strip().lower()
        gender = (row_ci.get("gender") or "").lower()
        p1 = _normalize_pos(row_ci.get("pref1") or row_ci.get("y-preferred-position") or "")
        p2 = _normalize_pos(row_ci.get("pref2") or "")
        p3 = _normalize_pos(row_ci.get("pref3") or "")
        if not name or not email:
            continue  # e-mail é obrigatório
        # Se tudo igual, manter só pref1
        if p1 and p1 == p2 == p3:
            p2 = ""
            p3 = ""
        out.append({"name": name, "gender": gender, "email": email, "pref1": p1, "pref2": p2, "pref3": p3})
    return out

# ----------------- Sheets I/O -------------------------
def ensure_tabs_and_headers():
    svc = sheets_service()
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()

    # normaliza títulos existentes
    titles_raw = [s["properties"]["title"] for s in meta.get("sheets", [])]
    titles_norm = { (t or "").strip().lower() for t in titles_raw }

    want_sessions = (SESSIONS_TAB or "Sessions").strip()
    want_assign   = (ASSIGNMENTS_TAB or "Assignments").strip()
    want_history  = "History"

    need_sessions = want_sessions.strip().lower() not in titles_norm
    need_assign   = want_assign.strip().lower()   not in titles_norm
    need_history  = want_history.strip().lower()  not in titles_norm

    requests = []
    if need_sessions:
        requests.append({"addSheet": {"properties": {"title": want_sessions}}})
    if need_assign:
        requests.append({"addSheet": {"properties": {"title": want_assign}}})
    if need_history:
        requests.append({"addSheet": {"properties": {"title": want_history}}})

    if requests:
        try:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body={"requests": requests}
            ).execute()
        except Exception as e:
            # Se outro processo criou a aba no meio tempo ou há variação de nome,
            # ignoramos erros de "already exists".
            msg = str(e).lower()
            if "already exists" not in msg:
                raise

    # ----- headers -----
    def header_ok(tab: str, expected: list[str]) -> bool:
        res = svc.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab}!A1:Z1"
        ).execute()
        vals = res.get("values", [])
        if not vals:
            return False
        row = [c.strip() for c in vals[0]]
        return row[:len(expected)] == expected

    if not header_ok(want_sessions, ["session_id", "date", "status"]):
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{want_sessions}!A1:C1",
            valueInputOption="RAW",
            body={"values": [["session_id", "date", "status"]]}
        ).execute()

    if not header_ok(want_assign, ["session_id", "name", "email", "pref1", "assigned_pos", "out_of_pref1"]):
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{want_assign}!A1:F1",
            valueInputOption="RAW",
            body={"values": [["session_id", "name", "email", "pref1", "assigned_pos", "out_of_pref1"]]}
        ).execute()

    if not header_ok(want_history, ["date", "name", "email", "pref1", "assigned_pos", "archived_at"]):
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{want_history}!A1:F1",
            valueInputOption="RAW",
            body={"values": [["date", "name", "email", "pref1", "assigned_pos", "archived_at"]]}
        ).execute()

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
        range=f"{ASSIGNMENTS_TAB}!A:F",  # 6 colunas, alinhado ao header
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

def _append_draft_session_with_assignments(session_id: str, session_date: str, teams: List[Dict], players_by_email: Dict[str, Dict]):
    """
    Store a DRAFT session and its assignments.
    - Mesmo dia: limpa Assignments e grava nova geração.
    - Mudou o dia: move off-pref do dia anterior para History (date-first), depois limpa Assignments.
    - Retenção do History: 5 datas.
    """
    svc = sheets_service()

    # Ler Assignments atuais
    res = svc.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"{ASSIGNMENTS_TAB}!A2:F"
    ).execute()
    existing = res.get("values", []) or []

    # Detectar data anterior pela session_id da primeira linha (prefixo YYYY-MM-DD)
    last_date = None
    if existing:
        first_sid = existing[0][0] if existing[0] else ""
        if first_sid and len(first_sid) >= 10:
            last_date = first_sid[:10]

    # Se mudou o dia → arquivar off-pref do dia anterior no History
    if last_date and last_date != session_date:
        offpref_rows = [r for r in existing if len(r) >= 6 and (r[5] or "").strip().lower() == "yes"]
        if offpref_rows:
            archived_at = datetime.utcnow().isoformat() + "Z"
            body = {
                "values": [[last_date, r[1], r[2], r[3], r[4], archived_at] for r in offpref_rows]  # date,name,email,pref1,assigned_pos,archived_at
            }
            svc.spreadsheets().values().append(
                spreadsheetId=GOOGLE_SHEET_ID,
                range="History!A:F",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()

        # Retenção do History: manter só 5 datas
        try:
            hres = svc.spreadsheets().values().get(
                spreadsheetId=GOOGLE_SHEET_ID, range="History!A2:F"
            ).execute()
            hrows = hres.get("values", []) or []
            dates = []
            for r in hrows:
                if r and len(r) >= 1 and (r[0] or "").strip():
                    dates.append((r[0].strip(), r))
            distinct = sorted({d for d, _ in dates})
            if len(distinct) > 5:
                keep = set(sorted(distinct, reverse=True)[:5])  # 5 datas mais recentes
                keep_rows = [r for r in hrows if r and (r[0] or "").strip() in keep]
                svc.spreadsheets().values().clear(
                    spreadsheetId=GOOGLE_SHEET_ID, range="History!A2:F"
                ).execute()
                if keep_rows:
                    svc.spreadsheets().values().update(
                        spreadsheetId=GOOGLE_SHEET_ID,
                        range="History!A2",
                        valueInputOption="RAW",
                        body={"values": keep_rows}
                    ).execute()
        except Exception:
            pass

    # Limpar Assignments e regravar do zero
    svc.spreadsheets().values().clear(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"{ASSIGNMENTS_TAB}!A2:F"
    ).execute()

    # Adicionar linha de sessão (controle)
    _append_sessions([[session_id, session_date, "DRAFT"]])

    # Construir linhas para Assignments (com email)
    rows = []
    for t in teams:
        for p in t.get("players", []):
            if p.get("is_missing"):
                continue
            name = p.get("name") or ""
            email = (p.get("email") or "").strip().lower()
            assigned = (p.get("pos") or "").strip().lower()
            pref1 = (players_by_email.get(email) or {}).get("pref1", "")
            out_flag = "yes" if (pref1 and assigned and pref1.lower() != assigned.lower()) else "no"
            rows.append([session_id, name, email, pref1, assigned, out_flag])

    _append_assignments(rows)

def _last_two_offpref_maps_by_email_from_history() -> Tuple[Dict[str, int], Dict[str, bool]]:
    """
    Lê a aba History (date, name, email, pref1, assigned_pos, archived_at),
    pega as DUAS últimas datas e computa:
      - count_map: email_id -> 0..2
      - any_map:   email_id -> True/False
    """
    svc = sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID, range="History!A2:F"
    ).execute()
    rows = res.get("values", []) or []

    # Agrupar por data (coluna 0)
    by_date: Dict[str, List[List[str]]] = defaultdict(list)
    for r in rows:
        if not r or len(r) < 5:
            continue
        date_str = (r[0] or "").strip()
        if not date_str:
            continue
        by_date[date_str].append(r)

    if not by_date:
        return {}, {}

    # Datas em ordem desc
    dates_sorted = sorted(by_date.keys(), reverse=True)
    last_two = dates_sorted[:2]

    count_map: Dict[str, int] = defaultdict(int)
    any_map: Dict[str, bool] = defaultdict(bool)

    for d in last_two:
        for r in by_date[d]:
            # r: [date, name, email, pref1, assigned_pos, archived_at]
            email = (r[2] if len(r) > 2 else "").strip().lower()
            pref1 = (r[3] if len(r) > 3 else "").strip().lower()
            assigned = (r[4] if len(r) > 4 else "").strip().lower()
            if not email:
                continue
            off = (pref1 and assigned and pref1 != assigned)
            if off:
                count_map[email] += 1
                any_map[email] = True
            else:
                any_map.setdefault(email, any_map[email])

    for k in list(count_map.keys()):
        count_map[k] = min(2, max(0, count_map[k]))
    return dict(count_map), dict(any_map)

# ----------------- Endpoints ---------------------------
@app.post("/upload-csv")
def upload_csv(file: UploadFile = File(...)):
    content = file.file.read()
    players = parse_csv_bytes(content)
    players = [p for p in players if p.get("name") and p.get("email")]
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
    Fairness: sempre baseado nas DUAS últimas datas do History (por e-mail).
    Esta geração é gravada como DRAFT e substitui Assignments do dia corrente.
    Body (opcional): {"seed": 123, "session_date": "YYYY-MM-DD"}
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

    # Fairness maps das DUAS últimas datas em History (por e-mail)
    try:
        last_two_count_map, last_two_any_map = _last_two_offpref_maps_by_email_from_history()
    except Exception:
        last_two_count_map, last_two_any_map = {}, {}

    teams = generate_teams(
        players,
        seed=seed,
        last_two_offpref_count_by_id=last_two_count_map,
        last_two_any_offpref_by_id=last_two_any_map
    )

    # Save como DRAFT
    session_id = f"{today.isoformat()}-{datetime.utcnow().strftime('%H%M%S')}"
    players_by_email = {(p.get("email") or "").strip().lower(): p for p in players}
    try:
        _append_draft_session_with_assignments(session_id, today.isoformat(), teams, players_by_email)
    except Exception:
        # não quebra a geração se falhar escrita no Sheets
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

    # Contagem por nome para detectar homônimos
    name_counts: Dict[str, int] = {}
    for t in teams:
        for p in t.get("players", []):
            if p.get("is_missing"):
                continue
            nm = p.get("name", "")
            name_counts[nm] = name_counts.get(nm, 0) + 1
    dup_names = {n for n, c in name_counts.items() if c > 1}

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
    ul { padding-left:18px; margin:8px 0 0 0; }
    li { margin:4px 0; }
    .pos { font-weight:bold; }
    .info { font-size:14px; color:#555; margin-top:4px; }
    .missing { color:red; font-weight:bold; margin-top:4px; }
    .extra { color:red; }
    @media (prefers-color-scheme: dark) {
      body { background:#111; color:#eee; }
      .card { border-color:#333; }
      .info { color:#aaa; }
    }
  </style>
</head>
<body>
  <h2>Teams</h2>
  <div class="grid">
"""
    for t in teams:
        # conta apenas jogadores reais (exclui placeholder)
        real_count = sum(1 for p in t.get("players", []) if not p.get("is_missing"))
        html += f'<div class="card"><h3 class="title">Time {t.get("team")}</h3>'
        html += f'<div class="info">Players: {real_count}</div>'
        if t.get("missing") == "middle":
            html += "<div class='missing'>Missing — middle</div>"
        html += "<ul>"

        extra_idx = t.get("extra_player_index")
        for idx, p in enumerate(t.get("players", [])):
            if p.get("is_missing"):
                # já mostramos “Missing — middle” acima, então pula aqui
                continue
            name = p.get("name", "")
            label = name
            if name in dup_names:
                em = (p.get("email") or "").strip()
                if em:
                    label = f'{name} &lt;{em}&gt;'
            li_class = "extra" if (t.get("size") == 7 and idx == extra_idx) else ""
            html += f"<li class='{li_class}'>{label} — <span class='pos'>{p.get('pos','')}</span></li>"

        html += "</ul></div>"

    html += """
  </div>
</body>
</html>
"""
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

# ----------------- Minimal Frontend (inalterado) --------------------
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