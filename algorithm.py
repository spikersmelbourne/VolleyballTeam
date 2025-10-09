# algorithm.py
# Pure team-generation logic (no network or filesystem I/O).
# Rules implemented:
# - Teams of 6 with layout: 1 setter, 2 middles, 3 outsides.
# - If a team has 5 players, the missing slots must be middles (shape: setter, middle, missing, outside, outside, outside).
# - Remainders:
#     +1 -> one team with 7 (extra as outside)
#     +2 -> ceil teams; one team with 4 (no middles), two teams with 5 (missing a middle), rest with 6
#     +3 -> three teams with 5 (missing a middle)
#     +4 -> two teams with 5 (missing a middle)
#     +5 -> one team with 5 (missing a middle)
# - Female distribution: try to spread ≥1 female per team if possible.
# - Female cannot be middle unless pref1 == middle (hard rule).
# - Fairness (last-two-final sessions, by player name):
#     If a player was off-pref in both of the last two sessions, do not assign them off-pref now.
# - Special case: if pref1=pref2=pref3='outside', prioritize this player to fill middle
#     when we need to use secondary/backfill — unless they already played off-pref in
#     either of the last two sessions (then do not use this special boost).
#
# Input format (players):
#   Each player is a dict: { "name": str, "gender": "m"|"f"|"" , "pref1": str, "pref2": str, "pref3": str }
#   Accepted positions: "setter", "middle", "outside" (any "oppo" should be normalized to "outside" before calling).
#
# Fairness maps:
#   last_two_offpref_count_by_name: dict[str, int] where 0..2 indicates how many times a player was off-pref across the last two FINAL sessions.
#   last_two_any_offpref_by_name: dict[str, bool] where True means the player was off-pref at least once in the last two FINAL sessions.
#
# Output format (teams):
#   A list like: [{ "team": 1, "players": [ {"name":..., "pos":..., "gender":...}, ... ] }, ...]
#
# This module is deterministic with an optional seed argument.



from __future__ import annotations
from typing import List, Dict, Tuple, Optional
import random

VALID_POS = {"setter", "middle", "outside"}


# ----- Helpers ------------------------------------------------------------------------------------

def _total_f(players: List[Dict]) -> int:
    """Count total female players."""
    return sum(1 for p in players if (p.get("gender") or "").lower() == "f")


def _team_has_f(team: Dict) -> bool:
    """Return True if team already has a female."""
    return any((pl.get("gender") or "").lower() == "f" for pl in team.get("players", []))


# ----- Templates ----------------------------------------------------------------------------------

def _templates_for_count(n_players: int) -> Tuple[int, List[List[str]], int]:
    """
    Decide team count and templates according to the remainder rules.
    Returns: (team_count, templates, extras_for_7)
    """
    r = n_players % 6
    if r == 0:
        T = n_players // 6
        templates = [["setter", "middle", "middle", "outside", "outside", "outside"] for _ in range(T)]
        return T, templates, 0
    if r in (1, 2):
        T = n_players // 6
        templates = [["setter", "middle", "middle", "outside", "outside", "outside"] for _ in range(T)]
        return T, templates, r
    # r = 3–5 → some 5-player teams missing one middle
    T = (n_players + 5) // 6
    five = {3: 3, 4: 2, 5: 1}[r]
    templates = []
    for i in range(T):
        if i < five:
            templates.append(["setter", "middle", "outside", "outside", "outside"])
        else:
            templates.append(["setter", "middle", "middle", "outside", "outside", "outside"])
    return T, templates, 0


# ----- Core selection logic -----------------------------------------------------------------------

def _eligible_rank_for_slot(
    player: Dict,
    pos: str,
    team_has_f_already: bool,
    distributed_f: bool,
    last_two_offpref_count_by_name: Dict[str, int],
    last_two_any_offpref_by_name: Dict[str, bool],
) -> Optional[Tuple[int, int]]:
    """
    Returns (pref_rank, penalty) if eligible, None if not.
    """
    name = player.get("name", "")
    gender = (player.get("gender") or "").lower()
    p1, p2, p3 = player.get("pref1", ""), player.get("pref2", ""), player.get("pref3", "")

    # Block female in middle unless pref1 == middle
    if pos == "middle" and gender == "f" and p1 != "middle":
        return None

    # Ensure 1 female per team before repeating
    if gender == "f" and team_has_f_already and not distributed_f:
        return None

    # Block players who went off-pref at least once in the last two FINAL sessions
    going_off = pos != p1
    if going_off and last_two_offpref_count_by_name.get(name, 0) >= 1:
        return None

    # Preference rank
    if p1 == pos:
        pref_rank = 1
    elif p2 == pos:
        pref_rank = 2
    elif p3 == pos:
        pref_rank = 3
    else:
        # special boost for all-outside when filling middle (if not off-pref recently)
        if pos == "middle" and p1 == p2 == p3 == "outside":
            if not last_two_any_offpref_by_name.get(name, False):
                pref_rank = 4
            else:
                pref_rank = 5
        else:
            pref_rank = 5

    penalty = 1 if (pos == "setter" and p1 == "middle" and p2 == "setter") else 0
    return (pref_rank, penalty)


# ----- Team generation ----------------------------------------------------------------------------

def generate_teams(
    players: List[Dict],
    *,
    seed: Optional[int] = None,
    last_two_offpref_count_by_name: Optional[Dict[str, int]] = None,
    last_two_any_offpref_by_name: Optional[Dict[str, bool]] = None,
) -> List[Dict]:
    """
    Create teams according to all rules.
    - Extras always become OUTSIDE (7th player)
    - Teams with <6 players are marked missing middle
    """
    if not players:
        return []

    if seed is not None:
        random.seed(seed)

    n = len(players)
    T, templates, extras_for_7 = _templates_for_count(n)
    teams = [{"team": i + 1, "players": []} for i in range(T)]
    total_f = _total_f(players)
    remaining = players[:]

    last_two_offpref_count_by_name = last_two_offpref_count_by_name or {}
    last_two_any_offpref_by_name = last_two_any_offpref_by_name or {}

    # Fill base templates
    for t_idx, tmpl in enumerate(templates):
        for pos in tmpl:
            team = teams[t_idx]
            teams_with_f = sum(1 for tm in teams if _team_has_f(tm))
            distributed_f = teams_with_f >= min(T, total_f)
            ranked: List[Tuple[Tuple[int, int], Dict]] = []
            for p in remaining:
                rank = _eligible_rank_for_slot(
                    p, pos,
                    team_has_f_already=_team_has_f(team),
                    distributed_f=distributed_f,
                    last_two_offpref_count_by_name=last_two_offpref_count_by_name,
                    last_two_any_offpref_by_name=last_two_any_offpref_by_name,
                )
                if rank is not None:
                    ranked.append((rank, p))
            if not ranked:
                continue
            ranked.sort(key=lambda t: (t[0][0], t[0][1], random.random()))
            pick = ranked[0][1]
            team["players"].append({
                "name": pick["name"],
                "pos": pos,
                "gender": (pick.get("gender") or "").lower(),
                "email": (pick.get("email") or "")
            })
            remaining.remove(pick)

    # Add extras (7th players always OUTSIDE)
    for _ in range(extras_for_7):
        if not remaining:
            break
        idx = min(range(T), key=lambda i: len(teams[i]["players"]))
        p = remaining.pop(0)
        teams[idx]["players"].append({
            "name": p["name"],
            "pos": "outside",
            "gender": (p.get("gender") or "").lower(),
            "email": (p.get("email") or "")
        })

    # Fill missing middle markers
    for team in teams:
        if len(team["players"]) < 6:
            team["missing"] = "middle"
        else:
            team["missing"] = None

    # Any leftovers → outside
    while remaining:
        idx = min(range(T), key=lambda i: len(teams[i]["players"]))
        p = remaining.pop(0)
        teams[idx]["players"].append({
            "name": p["name"],
            "pos": "outside",
            "gender": (p.get("gender") or "").lower(),
            "email": (p.get("email") or "")
        })

    return teams