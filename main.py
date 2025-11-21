# main.py — FastAPI + OOP services (CsvParser, SheetsRepository, TeamGenerator + postprocess)

from __future__ import annotations

import os
import base64
import json
import random
from typing import List, Dict, Optional
from datetime import datetime, date

import requests
from fastapi import FastAPI, UploadFile, File, Body
from fastapi.responses import HTMLResponse, JSONResponse

from csv_parser import CsvParser
from sheets_repository import SheetsRepository
from algorithm import TeamGenerator, postprocess_teams  # postprocess_teams será implementado no algorithm.py

# ----------------------- Config -----------------------
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SESSIONS_TAB = os.getenv("SESSIONS_TAB", "Sessions")
ASSIGNMENTS_TAB = os.getenv("ASSIGNMENTS_TAB", "Assignments")

app = FastAPI(title="Volleyball Teams — OOP Version")

STATE = {
    "players": [],    # [{name, gender, email, pref1, pref2, pref3}]
    "teams": [],      # [{team, size, missing, extra_player_index, players:[{name,email,gender,pos}]}]
    "source": "",
    "updated_at": None,
    "seed": None,
    "session_id": None,
}

csv_parser = CsvParser()
sheets_repo = SheetsRepository(
    spreadsheet_id=GOOGLE_SHEET_ID,
    sessions_tab=SESSIONS_TAB,
    assignments_tab=ASSIGNMENTS_TAB,
    history_tab="History",
    service_account_file="service_account.json",
)


# ----------------- Helpers ---------------------------
def _touch_state():
    STATE["updated_at"] = datetime.utcnow().isoformat() + "Z"


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


# ----------------- HTML snapshot helpers ------------------------

def build_teams_html(teams: List[Dict]) -> str:
    """Build the static HTML for the teams page."""
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
    h2 { margin-top: 0; }
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
        real_count = sum(1 for p in t.get("players", []) if not p.get("is_missing"))
        html += f'<div class="card"><h3 class="title">Time {t.get("team")}</h3>'
        html += f'<div class="info">Players: {real_count}</div>'
        if t.get("missing") == "middle":
            html += "<div class='missing'>Missing — middle</div>"
        html += "<ul>"

        extra_idx = t.get("extra_player_index")
        for idx, p in enumerate(t.get("players", [])):
            if p.get("is_missing"):
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

@app.get("/sessions/{session_date}/teams", response_class=HTMLResponse)
def show_teams_for_session_date(session_date: str):
    """
    Render teams for a given session_date (YYYY-MM-DD),
    reading them back from the Google Sheets Assignments tab.

    This does NOT regenerate teams; it only displays what was saved.
    """
    try:
        # garante tabs e headers
        sheets_repo.ensure_tabs_and_headers()
        # função nova que criamos no SheetsRepository
        teams = sheets_repo.get_teams_for_session(session_date)
    except Exception as e:
        return HTMLResponse(
            f"<p>Error loading teams for {session_date}: {e}</p>",
            status_code=500,
        )

    if not teams:
        return HTMLResponse(
            f"<p>No teams found for {session_date}. Generate and save teams first.</p>",
            status_code=404,
        )

    html = build_teams_html(teams)
    return html



def push_snapshot_to_github(html: str, date_str: str) -> None:
    """
    Push a static HTML snapshot for the given date to the GitHub Pages repo.
    - date_str: 'YYYY-MM-DD'
    """

    gh_token = os.getenv("GH_TOKEN", "").strip()
    gh_repo = os.getenv("GH_REPO", "").strip()
    gh_branch = os.getenv("GH_BRANCH", "main").strip()
    base_path = os.getenv("GH_BASE_PATH", "times").strip()

    if not gh_token or not gh_repo:
        return

    owner_repo = gh_repo  # formato 'user/repo'
    filename = f"{date_str}.html"
    path = f"{base_path}/{filename}"

    api_url = f"https://api.github.com/repos/{owner_repo}/contents/{path}"

    # Conteúdo em base64
    content_b64 = base64.b64encode(html.encode("utf-8")).decode("ascii")

    headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github+json",
    }

    # Descobrir SHA se o arquivo já existir (para update)
    sha = None
    try:
        get_resp = requests.get(api_url, headers=headers, params={"ref": gh_branch})
        if get_resp.status_code == 200:
            data = get_resp.json()
            sha = data.get("sha")
    except Exception:
        sha = None

    body = {
        "message": f"Update teams page for {date_str}",
        "content": content_b64,
        "branch": gh_branch,
    }
    if sha:
        body["sha"] = sha  # update

    try:
        requests.put(api_url, headers=headers, data=json.dumps(body))
    except Exception:
        pass


# ----------------- API: Control rules (GET/POST) --------------------

@app.get("/api/sessions/{session_key}/rules")
def get_session_rules(session_key: str):
    """
    Return all soft-control rules for a given session_key (e.g. '2025-11-20').
    Used by the /control page to load current rules of the day.
    """
    try:
        sheets_repo.ensure_tabs_and_headers()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Sheets error: {e}"}, status_code=500)

    try:
        raw_rules = sheets_repo.get_control_rules_for_session(session_key)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Error reading rules: {e}"}, status_code=500)

    # raw_rules já vem em formato estruturado do SheetsRepository
    return {"ok": True, "session_key": session_key, "rules": raw_rules}


@app.post("/api/sessions/{session_key}/rules")
def save_session_rules(session_key: str, payload: dict = Body(...)):
    """
    Save a snapshot of soft-control rules for a given session_key.
    The payload should be:
      {
        "session_key": "...",
        "rules": [
           {
             "player_email": "...",
             "cannot_play_positions": [...],
             "must_play_with": [...],
             "cannot_play_with": [...]
           },
           ...
        ]
      }
    """
    if not payload:
        return JSONResponse({"ok": False, "error": "Missing payload."}, status_code=400)

    body_session = (payload.get("session_key") or "").strip()
    if body_session and body_session != session_key:
        return JSONResponse(
            {"ok": False, "error": "session_key mismatch between URL and body."},
            status_code=400,
        )

    rules = payload.get("rules") or []
    if not isinstance(rules, list):
        return JSONResponse({"ok": False, "error": "'rules' must be a list."}, status_code=400)

    try:
        sheets_repo.ensure_tabs_and_headers()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Sheets error: {e}"}, status_code=500)

    try:
        normalized = sheets_repo.append_control_rules_snapshot(session_key, rules)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Error saving rules: {e}"}, status_code=500)

    return {"ok": True, "session_key": session_key, "rules": normalized}


# ----------------- Endpoints: CSV + Generate teams ---------------------------

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Step 1: receber CSV e guardar players no STATE, usando CsvParser.
    """
    content = await file.read()
    players = csv_parser.parse_players_from_bytes(content)
    players = [p for p in players if p.get("name") and p.get("email")]
    if not players:
        return JSONResponse({"ok": False, "error": "No players parsed from CSV."}, status_code=422)

    STATE["players"] = players
    STATE["teams"] = []
    STATE["source"] = f"upload:{file.filename}"
    _touch_state()
    return {
        "ok": True,
        "source": STATE["source"],
        "total_players": len(players),
        "players": players,
        "updated_at": STATE["updated_at"],
    }


@app.post("/generate-teams")
async def generate(payload: Optional[dict] = Body(default=None)):
    """
    Step 2: usar TeamGenerator + SheetsRepository + regras de controle.
    - lê STATE["players"]
    - usa histórico do Sheets para fairness
    - aplica keep_pref1 na geração (via keep_pref_emails)
    - aplica pós-processamento (must_play_with / cannot_play_with / cannot_play_positions)
    - salva sessão/assignments no Sheets
    - gera snapshot HTML no GitHub
    """
    players = STATE.get("players", [])
    if not players:
        return JSONResponse(
            {"ok": False, "error": "No players loaded. Upload a CSV first."},
            status_code=409,
        )

    # garantir abas e headers
    try:
        sheets_repo.ensure_tabs_and_headers()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Sheets error: {e}"}, status_code=500)

    # ---------- determinar date e seed ----------
    today = date.today()
    seed: Optional[int] = None

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

    # ---------- Regras de controle do dia → keep_pref_emails + session_rules ----------
    session_key = today.isoformat()
    try:
        session_rules = sheets_repo.get_control_rules_for_session(session_key)
    except Exception:
        session_rules = []

    keep_pref_emails: set[str] = set()
    for rule in session_rules:
        email = (rule.get("player_email") or "").strip().lower()
        if not email:
            continue
        # critério: se tem alguma posição em cannot_play_positions marcada,
        # tratamos esse jogador como "protegido" (keep pref1)
        cannot_positions = rule.get("cannot_play_positions") or []
        if cannot_positions:
            keep_pref_emails.add(email)

    # ---------- fairness das duas últimas datas ----------
    try:
        last_two_count_map, last_two_any_map = sheets_repo.get_last_two_offpref_maps_by_email()
    except Exception:
        last_two_count_map, last_two_any_map = {}, {}

    # ---------- gerar times via TeamGenerator (keep_pref_emails entra aqui) ----------
    generator = TeamGenerator(
        players,
        seed=seed,
        last_two_offpref_count_by_id=last_two_count_map,
        last_two_any_offpref_by_id=last_two_any_map,
        keep_pref_emails=keep_pref_emails,
    )
    teams = generator.generate()

    # ---------- pós-processamento de regras (must / cannot play with / cannot positions) ----------
    try:
        teams = postprocess_teams(
            teams=teams,
            session_rules=session_rules,
            last_two_offpref_count_by_id=last_two_count_map,
            last_two_any_offpref_by_id=last_two_any_map,
        )
    except Exception:
        # se der erro no pós-processo, seguimos com os times originais
        pass

    # ---------- salvar no Sheets ----------
    session_id = f"{today.isoformat()}-{datetime.utcnow().strftime('%H%M%S')}"
    players_by_email = {
        (p.get("email") or "").strip().lower(): p
        for p in players
    }
    try:
        sheets_repo.save_draft_session_with_assignments(
            session_id=session_id,
            session_date=today.isoformat(),
            teams=teams,
            players_by_email=players_by_email,
        )
    except Exception:
        # não quebra a geração se der erro de escrita
        pass

    # ---------- atualizar STATE ----------
    STATE["teams"] = teams
    STATE["seed"] = seed
    STATE["session_id"] = session_id
    _touch_state()

    # ---------- snapshot HTML → GitHub Pages ----------
    try:
        html_snapshot = build_teams_html(teams)
        date_str = today.isoformat()
        push_snapshot_to_github(html_snapshot, date_str)
    except Exception:
        pass

    return {
        "ok": True,
        "session_id": session_id,
        "teams": teams,
        "updated_at": STATE["updated_at"],
    }


@app.get("/teams", response_class=HTMLResponse)
def show_teams():
    teams = STATE.get("teams", [])
    if not teams:
        return "<p>No teams yet. Upload a CSV and click 'Generate Teams'.</p>"
    html = build_teams_html(teams)
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


# ----------------- Minimal Frontend (coach page) --------------------

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

  const r = await fetch('/generate-teams', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({})
  });

  const d = await r.json();
  btn.classList.remove('loading');
  btn.disabled = false;

  if(!d.ok){
    alert(d.error || 'Generate error');
    return;
  }

  const sessionId = d.session_id || '';
  const datePart = sessionId.substring(0, 10); // "2025-11-18"

  // Agora abrimos a página servida pelo PRÓPRIO backend,
  // que lê os times salvos no Google Sheets
  const url = `/sessions/${encodeURIComponent(datePart)}/teams`;
  window.open(url, '_blank');
}
</script>
  </body>
</html>
"""

# ----------------- Control page (admin) --------------------

@app.get("/control", response_class=HTMLResponse)
def control_panel():
    """
    Admin control page (simplified):
    - Session key = always today (YYYY-MM-DD), automatic.
    - One small form to add a rule:
        * player email
        * rule type (3 options)
          - keep_pref1  -> marked internally via cannot_play_positions = ["keep_pref1"]
          - must_play_with
          - cannot_play_with
    - A list showing current rules for today.
    """
    return """
<!doctype html>
<html>
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Control Rules - Simple</title>
    <style>
      body { font-family: system-ui, Arial; padding:16px; max-width: 720px; margin: 0 auto; }
      h2 { margin-top: 0; }
      label { font-size: 14px; display:block; margin-bottom:4px; }
      input[type="text"], select {
        padding:6px 8px;
        font-size:14px;
        border-radius:4px;
        border:1px solid #ccc;
        width: 100%;
        box-sizing: border-box;
      }
      .field { margin-bottom: 10px; }
      .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
      button {
        font-size: 14px;
        padding: 6px 12px;
        margin: 2px 0;
        border: 1px solid #ccc;
        border-radius: 6px;
        background: #f5f5f5;
        cursor: pointer;
        transition: background 0.2s, transform 0.05s;
      }
      button:hover { background:#e0e0e0; }
      button:active { background:#ccc; transform: scale(0.98); }
      button.small {
        font-size: 12px;
        padding: 3px 8px;
      }
      button.danger {
        border-color:#c62828;
        color:#c62828;
        background:#fff5f5;
      }
      .hint { font-size:12px; opacity:0.7; }
      #rulesList {
        margin-top:14px;
        padding-left:0;
        list-style:none;
        font-size:14px;
      }
      #rulesList li {
        border-bottom:1px solid #eee;
        padding:6px 0;
        display:flex;
        justify-content:space-between;
        align-items:center;
      }
      #status {
        margin-top:10px;
        font-size:13px;
        opacity:0.8;
      }
      @media (prefers-color-scheme: dark) {
        body { background:#111; color:#eee; }
        input[type="text"], select {
          background:#111;
          color:#eee;
          border-color:#444;
        }
        button { background:#222; color:#eee; border-color:#333; }
        button:hover { background:#333; }
        button.danger { background:#331111; border-color:#c62828; color:#ff8a80; }
        #rulesList li { border-color:#333; }
      }
    </style>
  </head>
  <body>
    <h2>Control Rules – Simple Admin</h2>

    <div class="field">
      <strong>Session (today):</strong>
      <span id="sessionKeyLabel"></span>
      <div class="hint">
        All rules created here will use today's date as session_key.
      </div>
    </div>

    <hr/>

    <h3>Add / update rule for a player</h3>

    <div class="field">
      <label for="playerEmail">Player email</label>
      <input type="text" id="playerEmail" placeholder="player@example.com" />
    </div>

    <div class="field">
      <label for="ruleType">Rule type</label>
      <select id="ruleType" onchange="onRuleTypeChange()">
        <option value="keep_pref1">Keep in first preference</option>
        <option value="must_play_with">Must play with</option>
        <option value="cannot_play_with">Cannot play with</option>
      </select>
      <div class="hint">
        "Keep in first preference" means we will strongly try to keep this player in their pref1 position.
      </div>
    </div>

    <div class="field" id="otherEmailField" style="display:none;">
      <label for="otherEmail">Other player email</label>
      <input type="text" id="otherEmail" placeholder="other@example.com" />
      <div class="hint">Used when rule type is "Must play with" or "Cannot play with".</div>
    </div>

    <div class="row" style="margin-top:8px;">
      <button onclick="addRule()">Add / update rule</button>
    </div>

    <hr/>

    <h3>Current rules for today</h3>
    <div class="hint">
      Each line below represents all rules for one player (email).
    </div>
    <ul id="rulesList"></ul>

    <div class="row" style="margin-top:10px;">
      <button onclick="saveRules()">Save all rules to Google Sheets</button>
    </div>

    <div id="status"></div>

<script>
let sessionKey = "";
// email -> { player_email, cannot_play_positions[], must_play_with[], cannot_play_with[], keep_pref1: bool }
let rulesByEmail = {};

function setStatus(msg, isError=false) {
  const el = document.getElementById('status');
  el.innerText = msg || '';
  el.style.color = isError ? '#c62828' : '';
}

function onRuleTypeChange() {
  const type = document.getElementById('ruleType').value;
  const otherField = document.getElementById('otherEmailField');

  if (type === 'keep_pref1') {
    otherField.style.display = 'none';
  } else {
    otherField.style.display = 'block';
  }
}

function refreshRulesList() {
  const ul = document.getElementById('rulesList');
  ul.innerHTML = '';

  const emails = Object.keys(rulesByEmail).sort();
  if (!emails.length) {
    const li = document.createElement('li');
    li.innerText = 'No rules for today yet.';
    ul.appendChild(li);
    return;
  }

  for (const email of emails) {
    const rule = rulesByEmail[email];
    const li = document.createElement('li');

    const left = document.createElement('div');
    const parts = [];

    if (rule.keep_pref1) {
      parts.push('keep in first preference');
    }
    if (rule.must_play_with && rule.must_play_with.length) {
      parts.push(`must play with: ${rule.must_play_with.join(',')}`);
    }
    if (rule.cannot_play_with && rule.cannot_play_with.length) {
      parts.push(`cannot play with: ${rule.cannot_play_with.join(',')}`);
    }

    const text = parts.length ? parts.join(' | ') : '(no constraints)';
    left.innerHTML = `<strong>${email}</strong><br/><span>${text}</span>`;

    const right = document.createElement('div');
    const btnRemove = document.createElement('button');
    btnRemove.innerText = 'Remove';
    btnRemove.className = 'small danger';
    btnRemove.onclick = () => {
      delete rulesByEmail[email];
      refreshRulesList();
    };
    right.appendChild(btnRemove);

    li.appendChild(left);
    li.appendChild(right);
    ul.appendChild(li);
  }
}

function addRule() {
  const emailInput = document.getElementById('playerEmail');
  const typeSelect = document.getElementById('ruleType');
  const otherEmailInput = document.getElementById('otherEmail');

  const email = (emailInput.value || '').trim().toLowerCase();
  if (!email) {
    alert('Inform the player email.');
    return;
  }

  if (!rulesByEmail[email]) {
    rulesByEmail[email] = {
      player_email: email,
      cannot_play_positions: [],
      must_play_with: [],
      cannot_play_with: [],
      keep_pref1: false,
    };
  }

  const type = typeSelect.value;

  if (type === 'keep_pref1') {
    // Marcamos como "keep pref1" e usamos cannot_play_positions APENAS como flag
    rulesByEmail[email].keep_pref1 = true;
    rulesByEmail[email].cannot_play_positions = ['keep_pref1'];
  } else if (type === 'must_play_with') {
    const other = (otherEmailInput.value || '').trim().toLowerCase();
    if (!other) {
      alert('Inform the other player email.');
      return;
    }
    if (!rulesByEmail[email].must_play_with.includes(other)) {
      rulesByEmail[email].must_play_with.push(other);
    }
  } else if (type === 'cannot_play_with') {
    const other = (otherEmailInput.value || '').trim().toLowerCase();
    if (!other) {
      alert('Inform the other player email.');
      return;
    }
    if (!rulesByEmail[email].cannot_play_with.includes(other)) {
      rulesByEmail[email].cannot_play_with.push(other);
    }
  }

  otherEmailInput.value = '';
  refreshRulesList();
  setStatus('Rule added/updated locally. Remember to click "Save all rules".');
}

async function loadRulesForToday() {
  if (!sessionKey) return;
  setStatus('Loading rules for today...');

  try {
    const resp = await fetch(`/api/sessions/${encodeURIComponent(sessionKey)}/rules`);
    if (!resp.ok) {
      setStatus('Error loading rules: ' + resp.status, true);
      return;
    }
    const data = await resp.json();
    const rules = data.rules || [];
    rulesByEmail = {};

    for (const r of rules) {
      const email = (r.player_email || '').toLowerCase();
      if (!email) continue;

      const cps = r.cannot_play_positions || [];
      const keepPref1 = Array.isArray(cps) && cps.length > 0; // qualquer valor → flag de keep_pref1

      rulesByEmail[email] = {
        player_email: email,
        cannot_play_positions: cps,
        must_play_with: r.must_play_with || [],
        cannot_play_with: r.cannot_play_with || [],
        keep_pref1: keepPref1,
      };
    }

    refreshRulesList();
    setStatus(`Loaded ${rules.length} rule(s) for today.`);
  } catch (err) {
    console.error(err);
    setStatus('Error loading rules (network or server).', true);
  }
}

async function saveRules() {
  if (!sessionKey) {
    setStatus('No session key (today) detected.', true);
    return;
  }

  const rules = Object.values(rulesByEmail).map(r => ({
    player_email: r.player_email,
    cannot_play_positions: r.cannot_play_positions || [],
    must_play_with: r.must_play_with || [],
    cannot_play_with: r.cannot_play_with || [],
  }));

  setStatus('Saving rules...');

  try {
    const resp = await fetch(`/api/sessions/${encodeURIComponent(sessionKey)}/rules`, {
      method:'POST',
      headers:{ 'Content-Type':'application/json' },
      body: JSON.stringify({
        session_key: sessionKey,
        rules: rules
      })
    });

    if (!resp.ok) {
      setStatus('Error saving rules: ' + resp.status, true);
      return;
    }

    const data = await resp.json();
    const savedCount = (data.rules || []).length;
    setStatus(`Saved snapshot with ${savedCount} rule(s) for session ${sessionKey}.`);
  } catch (err) {
    console.error(err);
    setStatus('Error saving rules (network or server).', true);
  }
}

(function init() {
  try {
    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth() + 1).padStart(2, '0');
    const dd = String(today.getDate()).padStart(2, '0');
    sessionKey = `${yyyy}-${mm}-${dd}`;
    document.getElementById('sessionKeyLabel').innerText = sessionKey;
  } catch (e) {
    sessionKey = '';
  }

  onRuleTypeChange();
  loadRulesForToday();
})();
</script>

  </body>
</html>
"""