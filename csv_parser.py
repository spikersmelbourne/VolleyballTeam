# csv_parser.py
from __future__ import annotations
from typing import List, Dict, Optional
import csv
import io


class CsvParser:
    """Service responsible for parsing CSV bytes into player dicts."""

    @staticmethod
    def _norm(s: Optional[str]) -> str:
        return (s or "").strip().lower()

    @staticmethod
    def _normalize_pos(x: str) -> str:
        """
        Normalize position strings coming from the CSV/Form.

        Accepts variations like:
        - "Outside", "Oppo", "Outside/Oppo", "Outside hitter", etc → "outside"
        - "Middle", "Middle blocker" → "middle"
        - "Setter" → "setter"
        Anything else becomes empty string (invalid position).
        """
        v = CsvParser._norm(x)
        if not v:
            return ""
        # Outside / opposite variants
        if "outside" in v or "oppo" in v or "opposite" in v:
            return "outside"
        # Middle variants
        if "middle" in v:
            return "middle"
        # Setter variants
        if "setter" in v:
            return "setter"
        return ""

    def parse_players_from_bytes(self, b: bytes) -> List[Dict[str, str]]:
        """
        Parse CSV bytes and return a list of player dicts with
        name, email, gender, pref1, pref2, pref3.
        """
        text = b.decode("utf-8", errors="ignore")
        out: List[Dict[str, str]] = []
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            # normalize headers to lowercase
            row_ci = { (k or '').strip().lower(): (v or '').strip() for k, v in row.items() }

            # Name (same logic you had)
            name = row_ci.get("name") or row_ci.get("player") or row_ci.get("jogador")

            # Email
            email = (row_ci.get("email") or row_ci.get("e-mail") or row_ci.get("mail") or "").strip().lower()

            # Gender (if exists)
            gender = (row_ci.get("gender") or "").lower()

            # Preferred positions
            p1 = self._normalize_pos(row_ci.get("y-preferred-position-1") or "")
            p2 = self._normalize_pos(row_ci.get("y-preferred-position-2") or "")
            p3 = self._normalize_pos(row_ci.get("y-preferred-position-3") or "")

            if not name or not email:
                continue  # keep your rule: must have name and email

            # if all three prefs are equal, clear p2/p3
            if p1 and p1 == p2 == p3:
                p2 = ""
                p3 = ""

            out.append({
                "name": name,
                "gender": gender,
                "email": email,
                "pref1": p1,
                "pref2": p2,
                "pref3": p3,
            })

        return out