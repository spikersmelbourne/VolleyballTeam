# sheets_repository.py
from __future__ import annotations
from typing import List, Dict, Tuple, Optional
from datetime import datetime
from collections import defaultdict
import os

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as ServiceAccountCredentials


# ---------- Control Rules config & helpers ----------

# Nome padrão da aba de regras de controle (pode ser sobrescrito por env var)
CONTROL_RULES_TAB = os.getenv("CONTROL_RULES_TAB", "ControlRules")


def _split_csv_field(value: str) -> List[str]:
    """
    Convert a comma-separated string like 'middle,outside'
    into ['middle', 'outside']. Empty or None -> [].
    """
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _join_csv_field(values: List[str]) -> str:
    """
    Convert a list like ['middle', 'outside'] into 'middle,outside'.
    Empty list -> ''.
    """
    if not values:
        return ""
    return ",".join(values)


class SheetsRepository:
    """
    Service responsible for all interactions with Google Sheets:
    - tabs and headers
    - append sessions and assignments
    - read history and compute fairness maps
    - (v2) read/write control rules per session
    """

    def __init__(
        self,
        spreadsheet_id: str,
        *,
        sessions_tab: str = "Sessions",
        assignments_tab: str = "Assignments",
        history_tab: str = "History",
        service_account_file: str = "service_account.json",
        control_rules_tab: str = CONTROL_RULES_TAB,
    ):
        self.spreadsheet_id = spreadsheet_id.strip()
        self.sessions_tab = sessions_tab
        self.assignments_tab = assignments_tab
        self.history_tab = history_tab
        self.service_account_file = service_account_file

        # Aba para regras de controle (ControlRules)
        self.control_rules_tab = control_rules_tab

        self._service = None  # lazy-loaded Google Sheets service

    # ---------- Internal helpers ----------

    def _credentials(self):
        """Load service account credentials from the JSON key file."""
        if not os.path.exists(self.service_account_file):
            raise RuntimeError("service_account.json not found. Place it in the project folder.")
        creds = ServiceAccountCredentials.from_service_account_file(
            self.service_account_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return creds

    def _sheets_service(self):
        if self._service is None:
            if not self.spreadsheet_id:
                raise RuntimeError("Spreadsheet ID not configured.")
            # cache_discovery=False avoids some local warnings
            self._service = build("sheets", "v4", credentials=self._credentials(), cache_discovery=False)
        return self._service

    # ---------- Tabs & headers ----------

    def ensure_tabs_and_headers(self):
        """
        Garante as abas principais (Sessions, Assignments, History) e seus headers.
        A aba de regras (ControlRules) é garantida separadamente em
        _ensure_control_rules_header(), chamada quando necessário.
        """
        svc = self._sheets_service()
        meta = svc.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()

        # normalize existing titles
        titles_raw = [s["properties"]["title"] for s in meta.get("sheets", [])]
        titles_norm = {(t or "").strip().lower() for t in titles_raw}

        want_sessions = (self.sessions_tab or "Sessions").strip()
        want_assign = (self.assignments_tab or "Assignments").strip()
        want_history = (self.history_tab or "History").strip()

        need_sessions = want_sessions.strip().lower() not in titles_norm
        need_assign = want_assign.strip().lower() not in titles_norm
        need_history = want_history.strip().lower() not in titles_norm

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
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": requests}
                ).execute()
            except Exception as e:
                msg = str(e).lower()
                if "already exists" not in msg:
                    raise

        # headers
        def header_ok(tab: str, expected: list[str]) -> bool:
            res = svc.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range=f"{tab}!A1:Z1"
            ).execute()
            vals = res.get("values", [])
            if not vals:
                return False
            row = [c.strip() for c in vals[0]]
            return row[:len(expected)] == expected

        if not header_ok(want_sessions, ["session_id", "date", "status"]):
            svc.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{want_sessions}!A1:C1",
                valueInputOption="RAW",
                body={"values": [["session_id", "date", "status"]]}
            ).execute()

        if not header_ok(want_assign, ["session_id", "team", "name", "email", "pref1", "assigned_pos", "out_of_pref1"]):
            svc.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{want_assign}!A1:G1",
                valueInputOption="RAW",
                body={"values": [["session_id", "team", "name", "email", "pref1", "assigned_pos", "out_of_pref1"]]}
            ).execute()

        if not header_ok(want_history, ["date", "name", "email", "pref1", "assigned_pos", "archived_at"]):
            svc.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{want_history}!A1:F1",
                valueInputOption="RAW",
                body={"values": [["date", "name", "email", "pref1", "assigned_pos", "archived_at"]]}
            ).execute()

        # A aba de ControlRules (se ainda não existir) será criada on-demand
        # em _ensure_control_rules_header().

    # ---------- Low-level append helpers ----------

    def _append_sessions(self, rows: List[List[str]]):
        if not rows:
            return
        svc = self._sheets_service()
        svc.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.sessions_tab}!A:C",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()

    def _append_assignments(self, rows: List[List[str]]):
        if not rows:
            return
        svc = self._sheets_service()
        svc.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.assignments_tab}!A:G",  # 7 columns
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()

    # ---------- History / Fairness ----------

    def get_last_two_offpref_maps_by_email(self) -> Tuple[Dict[str, int], Dict[str, bool]]:
        """
        Read the History tab and compute:
          - count_map: email -> 0..2
          - any_map:   email -> True/False
        using the last two dates.
        """
        svc = self._sheets_service()
        res = svc.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=f"{self.history_tab}!A2:F"
        ).execute()
        rows = res.get("values", []) or []

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

    def save_draft_session_with_assignments(
        self,
        session_id: str,
        session_date: str,
        teams: List[Dict],
        players_by_email: Dict[str, Dict],
    ) -> None:
        """
        Store a DRAFT session and its assignments.

        - If date changed: archive previous day's off-pref assignments into History.
        - Clear Assignments tab and write new rows for this session.
        - Append a new row into Sessions tab.
        """
        svc = self._sheets_service()

        # Read existing assignments
        res = svc.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.assignments_tab}!A2:G"
        ).execute()
        existing = res.get("values", []) or []

        # Detect previous date from first session_id (prefix YYYY-MM-DD)
        last_date = None
        if existing:
            first_sid = existing[0][0] if existing[0] else ""
            if first_sid and len(first_sid) >= 10:
                last_date = first_sid[:10]

                # If date changed → archive previous day's off-pref rows into History
        if last_date and last_date != session_date:
            offpref_rows = [
                r for r in existing
                if len(r) >= 7 and (r[6] or "").strip().lower() == "yes"
            ]
            if offpref_rows:
                archived_at = datetime.utcnow().isoformat() + "Z"

                # existing row layout (Assignments):
                # [session_id, team, name, email, pref1, assigned_pos, out_of_pref1]
                body = {
                    "values": [
                        [
                            last_date,      # date
                            r[2],           # name
                            r[3],           # email
                            r[4],           # pref1
                            r[5],           # assigned_pos
                            archived_at     # archived_at
                        ]
                        for r in offpref_rows
                    ]
                }

                svc.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.history_tab}!A:F",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                ).execute()
                svc.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.history_tab}!A:F",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body=body
                ).execute()

            # Optional: keep only last 5 dates in History
            try:
                hres = svc.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.history_tab}!A2:F"
                ).execute()
                hrows = hres.get("values", []) or []
                dates = []
                for r in hrows:
                    if r and len(r) >= 1 and (r[0] or "").strip():
                        dates.append((r[0].strip(), r))
                distinct = sorted({d for d, _ in dates})
                if len(distinct) > 5:
                    keep = set(sorted(distinct, reverse=True)[:5])
                    keep_rows = [r for r in hrows if r and (r[0] or "").strip() in keep]
                    svc.spreadsheets().values().clear(
                        spreadsheetId=self.spreadsheet_id,
                        range=f"{self.history_tab}!A2:F"
                    ).execute()
                    if keep_rows:
                        svc.spreadsheets().values().update(
                            spreadsheetId=self.spreadsheet_id,
                            range=f"{self.history_tab}!A2",
                            valueInputOption="RAW",
                            body={"values": keep_rows}
                        ).execute()
            except Exception:
                pass

        # Clear Assignments and re-write
        svc.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.assignments_tab}!A2:G"
        ).execute()

        # Build assignment rows
        rows = []
        for t in teams:
            team_number = t.get("team")  # número do time (int)
            for p in t.get("players", []):
                if p.get("is_missing"):
                    continue
                name = p.get("name") or ""
                email = (p.get("email") or "").strip().lower()
                assigned = (p.get("pos") or "").strip().lower()
                pref1 = (players_by_email.get(email) or {}).get("pref1", "")
                out_flag = "yes" if (pref1 and assigned and pref1.lower() != assigned.lower()) else "no"

                rows.append(
                    [
                        session_id,          # session_id
                        team_number,         # team
                        name,                # name
                        email,               # email
                        pref1,               # pref1
                        assigned,            # assigned_pos
                        out_flag,            # out_of_pref1
                    ]
                )

        self._append_assignments(rows)

    def get_teams_for_session(self, session_id: str) -> List[Dict]:
        """
        Reconstrói os times de uma sessão específica a partir da aba Assignments.

        Retorna uma lista de dicts no formato esperado por build_teams_html:
        [
            {
                "team": 1,
                "size": 6,
                "missing": None ou "middle",
                "extra_player_index": None ou int,
                "players": [
                    {
                        "name": ...,
                        "email": ...,
                        "pref1": ...,
                        "pos": ...,
                        "out_of_pref1": "yes"/"no",
                        "is_missing": False
                    },
                    ...
                ]
            },
            ...
        ]
        """
        svc = self._sheets_service()
        # lê tudo (header + linhas)
        resp = svc.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.assignments_tab}!A1:G"
        ).execute()
        values = resp.get("values", []) or []
        if len(values) < 2:
            return []

        header = values[0]
        rows = values[1:]

        # tenta localizar os índices das colunas importantes
        def idx(col_name: str) -> int:
            try:
                return header.index(col_name)
            except ValueError:
                return -1

        idx_sid = idx("session_id")
        idx_team = idx("team")
        idx_name = idx("name")
        idx_email = idx("email")
        idx_pref1 = idx("pref1")
        idx_pos = idx("assigned_pos")
        idx_out = idx("out_of_pref1")

        if min(idx_sid, idx_team, idx_name, idx_email, idx_pref1, idx_pos, idx_out) < 0:
            # header não está no formato esperado
            return []

        grouped: Dict[int, List[Dict]] = {}

        for r in rows:
            # garante tamanho mínimo
            if len(r) <= max(idx_sid, idx_team, idx_name, idx_email, idx_pref1, idx_pos, idx_out):
                continue

            sid = (r[idx_sid] or "").strip()
            if not (sid == session_id or sid.startswith(session_id + "-")):
                continue

            team_str = (r[idx_team] or "").strip()
            if not team_str:
                continue

            try:
                team_number = int(team_str)
            except ValueError:
                continue

            player = {
                "name": r[idx_name] if idx_name >= 0 else "",
                "email": (r[idx_email] if idx_email >= 0 else "").strip().lower(),
                "pref1": (r[idx_pref1] if idx_pref1 >= 0 else "").strip().lower(),
                "pos": (r[idx_pos] if idx_pos >= 0 else "").strip().lower(),
                "out_of_pref1": (r[idx_out] if idx_out >= 0 else "").strip().lower(),
                "is_missing": False,
            }

            grouped.setdefault(team_number, []).append(player)

        teams: List[Dict] = []
        for team_number in sorted(grouped.keys()):
            players = grouped[team_number]
            size = len(players)

            # regra visual: times de 5 mostram "Missing — middle"
            if size == 5:
                missing = "middle"
            else:
                missing = None

            # extra_player_index: como não sabemos qual era o "extra" original,
            # deixamos None (sem destaque visual especial)
            teams.append(
                {
                    "team": team_number,
                    "size": size,
                    "missing": missing,
                    "extra_player_index": None,
                    "players": players,
                }
            )

        return teams

        # ---------- Control Rules (per session, per player) ----------

    def _ensure_control_rules_header(self) -> None:
        """
        Garante que a aba ControlRules exista e tenha cabeçalho:
        session_key | player_email | cannot_play_positions | must_play_with |
        cannot_play_with | forced_position | comment
        """
        svc = self._sheets_service()

        # Verifica se a aba existe
        meta = svc.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        sheets = meta.get("sheets", []) or []
        titles = [(s["properties"]["title"] or "").strip() for s in sheets]
        if self.control_rules_tab not in titles:
            # Cria a aba se não existir
            try:
                svc.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": [{"addSheet": {"properties": {"title": self.control_rules_tab}}}]}
                ).execute()
            except Exception as e:
                msg = str(e).lower()
                if "already exists" not in msg:
                    raise

        # Garante o cabeçalho
        range_name = f"{self.control_rules_tab}!A1:G1"
        resp = svc.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=range_name
        ).execute()
        values = resp.get("values", [])

        expected_header = [
            "session_key",
            "player_email",
            "cannot_play_positions",
            "must_play_with",
            "cannot_play_with",
            "forced_position",
            "comment",
        ]

        if not values:
            svc.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body={"values": [expected_header]},
            ).execute()
            return

        current_header = values[0]
        if (
            len(current_header) >= 2
            and current_header[0] == "session_key"
            and current_header[1] == "player_email"
        ):
            return

        svc.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": [expected_header]},
        ).execute()


    def get_control_rules_for_session(self, session_key: str) -> List[Dict]:
        """
        Read all control rules for a given session_key from the ControlRules tab.

        Returns a list of dicts with:
        player_email, cannot_play_positions, must_play_with,
        cannot_play_with, forced_position
        """
        svc = self._sheets_service()
        self._ensure_control_rules_header()

        range_name = f"{self.control_rules_tab}!A2:G"
        resp = svc.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=range_name
        ).execute()
        values = resp.get("values", [])

        if not values:
            return []

        rules_by_email: Dict[str, Dict] = {}

        for row in values:
            if not row or len(row) < 2:
                continue

            sk = (row[0] or "").strip()
            if sk != session_key:
                continue

            email = (row[1] or "").strip().lower()
            if not email:
                continue

            cannot_positions_raw = row[2] if len(row) > 2 else ""
            must_with_raw = row[3] if len(row) > 3 else ""
            cannot_with_raw = row[4] if len(row) > 4 else ""
            forced_raw = row[5] if len(row) > 5 else ""
            comment_raw = row[6] if len(row) > 6 else ""

            cannot_positions = _split_csv_field(cannot_positions_raw)
            must_with = _split_csv_field(must_with_raw)
            cannot_with = _split_csv_field(cannot_with_raw)

            # forced_position pode vir da coluna 5 ou da comment
            forced_position = (forced_raw or comment_raw or "").strip().lower()

            rules_by_email[email] = {
                "player_email": email,
                "cannot_play_positions": cannot_positions,
                "must_play_with": must_with,
                "cannot_play_with": cannot_with,
                "forced_position": forced_position,
            }

        return list(rules_by_email.values())


    def append_control_rules_snapshot(self, session_key: str, rules: List[Dict]) -> None:
        """
        Save a snapshot in the ControlRules tab.
        The last line for each email/session_key overrides previous ones.
        """
        if not rules:
            return

        svc = self._sheets_service()
        self._ensure_control_rules_header()

        body_values = []
        for r in rules:
            email = (r.get("player_email") or "").strip().lower()
            if not email:
                continue

            cannot_positions = _join_csv_field(r.get("cannot_play_positions", []))
            must_with = _join_csv_field(r.get("must_play_with", []))
            cannot_with = _join_csv_field(r.get("cannot_play_with", []))
            forced_position = (r.get("forced_position") or "").strip().lower()
            comment = r.get("comment") or ""

            body_values.append([
                session_key,
                email,
                cannot_positions,
                must_with,
                cannot_with,
                forced_position,
                comment,
            ])

        if not body_values:
            return

        svc.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.control_rules_tab}!A:G",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": body_values},
        ).execute()