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
    return any((pl.get("gender") or "").lower() == "f" for pl in team.get("players", []) if not pl.get("is_missing"))


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


# ----- Templates (planejamento por N % 6, com ordem 7 → 6 → 5) -----------------------------------

def _templates_for_count(n_players: int) -> Tuple[int, List[List[str]]]:
    """
    Decide team count and templates according to remainder rules.
    - r=0: T times de 6
    - r=1,2: floor(N/6) times de 6 e r times de 7  → total = floor + r (ordem: 7,7,... depois 6)
    - r=3: ceil times; 3 de 5
    - r=4: ceil times; 2 de 5
    - r=5: ceil times; 1 de 5
    Ordem final das templates: todos 7 primeiro, depois 6, por fim 5.
    """
    r = n_players % 6
    if r == 0:
        T6 = n_players // 6
        templates = [["setter", "middle", "middle", "outside", "outside", "outside"] for _ in range(T6)]
        return T6, templates

    if r in (1, 2):
        base = n_players // 6
        T = base   # <-- CORREÇÃO: total de times é base + r
        templates_7 = [["setter", "middle", "middle", "outside", "outside", "outside", "outside"] for _ in range(r)]
        templates_6 = [["setter", "middle", "middle", "outside", "outside", "outside"] for _ in range(T - r)]
        templates = templates_7 + templates_6   # 7 primeiro, depois 6
        return T, templates

    # r in (3, 4, 5) → alguns times de 5 (falta 1 middle)
    T = (n_players + 5) // 6  # ceil
    five = {3: 3, 4: 2, 5: 1}[r]
    # ordem: 6 primeiro, depois 5 (para que 5 sejam os últimos)
    templates_6 = [["setter", "middle", "middle", "outside", "outside", "outside"] for _ in range(T - five)]
    templates_5 = [["setter", "middle", "outside", "outside", "outside"] for _ in range(five)]  # 5: {1S,1M,3O}
    templates = templates_6 + templates_5
    return T, templates


# ----- Elegibilidade / ranking (com fairness "suave" e 2 passes) ---------------------------------
def _rank_for_slot(
    player: Dict,
    pos: str,
    *,
    team_has_f_already: bool,
    distributed_f: bool,
    last_two_offpref_count_by_id: Dict[str, int],
    last_two_any_offpref_by_id: Dict[str, bool],
    relaxed: bool,
) -> Optional[Tuple[int, int, int]]:
    """Return (pref_rank, fairness_penalty, special_penalty) if the player is eligible
    for the given slot, or None if they cannot be used there.

    Regras principais:
      - F só joga middle se pref1 == "middle".
      - Fairness é baseado em quantas vezes a pessoa jogou fora da pref1
        nas duas últimas datas (off-pref), independente da posição.
    """

    email = _norm(player.get("email"))
    gender = _norm(player.get("gender"))
    p1, p2, p3 = _norm(player.get("pref1")), _norm(player.get("pref2")), _norm(player.get("pref3"))

    # Quantas vezes essa pessoa já foi off-pref nas duas últimas datas
    off_ct = last_two_offpref_count_by_id.get(email, 0)

    all_setter = (p1, p2, p3) == ("setter", "setter", "setter")

    # ---------------------------------------------------
    #  BLOCO ESPECÍFICO PARA MIDDLE (onde entra o sacrifício)
    # ---------------------------------------------------
    if pos == "middle":
        # 1) F só joga middle se pref1 == "middle"
        if gender == "f" and p1 != "middle":
            return None

        # 2) Se já foi off-pref nas DUAS últimas datas, não sacrificar de novo
        if not relaxed and off_ct >= 2:
            return None

        # 3) Verdadeiros middles têm prioridade
        if p1 == "middle":
            pref_rank = 1
            going_off = False
        else:
            # 4) BACKFILL: qualquer outro jogador elegível tem a MESMA prioridade
            #    (não importa o que colocou em pref2/pref3)
            pref_rank = 4
            going_off = True  # está indo fora da pref1

        # 5) Fairness penalty: maior para quem já foi off-pref antes
        fairness_penalty = 0
        if going_off:
            fairness_penalty += off_ct * 2
            if off_ct >= 1 and not relaxed:
                fairness_penalty += 3

        special_penalty = 0
        return (pref_rank, fairness_penalty, special_penalty)

    # ---------------------------------------------------
    #  OUTRAS POSIÇÕES (setter / outside / oppo)
    # ---------------------------------------------------

    # Distribuição de F: tenta manter no máx. 1 F por time até todos terem uma
    if not relaxed and gender == "f" and team_has_f_already and not distributed_f:
        return None

    # Regra de setter: evitar usar quem é "middle" de pref1 como setter,
    # a menos que setter esteja em pref2.
    if pos == "setter" and not relaxed and p1 == "middle" and p2 != "setter":
        return None

    # Ranking de preferência (1 = melhor)
    if pos == "setter" and all_setter:
        pref_rank = 0
    elif p1 == pos:
        pref_rank = 1
    elif p2 == pos:
        pref_rank = 2
    elif p3 == pos:
        pref_rank = 3
    else:
        pref_rank = 5

    # Fairness: penaliza quem vai jogar fora de pref1
    going_off = (pos != p1)
    fairness_penalty = 0
    if going_off:
        fairness_penalty += off_ct * 2
        if not relaxed and off_ct >= 1:
            fairness_penalty += 3

    # Penalidade leve para conversão middle → setter (quando permitido)
    special_penalty = 1 if (pos == "setter" and p1 == "middle" and p2 == "setter") else 0

    return (pref_rank, fairness_penalty, special_penalty)


# ----- Team generation ----------------------------------------------------------------------------

def generate_teams(
    players: List[Dict],
    *,
    seed: Optional[int] = None,
    last_two_offpref_count_by_id: Optional[Dict[str, int]] = None,
    last_two_any_offpref_by_id: Optional[Dict[str, bool]] = None,
) -> List[Dict]:
    """
    Entrada esperada já normalizada (name/email/gender/pref1..3).
    Saída:
      [
        {
          "team": 1,
          "size": 6|7|5,
          "missing": None|"middle",
          "extra_player_index": Optional[int],  # índice do 7º jogador para destacar em vermelho
          "players": [
             {"name":..., "email":..., "gender":..., "pos":"setter|middle|outside", "is_missing": bool?}
          ],
          "meta": {"setter":1, "middle":1|2, "outside":3|4}
        },
        ...
      ]
    """
    if not players:
        return []

    # seed e randomização estável
    if seed is not None:
        random.seed(seed)
    players = players[:]  # cópia
    random.shuffle(players)  # evita viés de ordem

    n = len(players)
    T, templates = _templates_for_count(n)

    # times em ordem: 7 → 6 → 5 (porque os templates já vêm nessa ordem)
    teams: List[Dict] = []
    for i, tmpl in enumerate(templates):
        size = len(tmpl)
        meta = {
            "setter": 1,
            "middle": 2 if size in (6, 7) else 1,
            "outside": 4 if size == 7 else 3
        }
        teams.append({
            "team": i + 1,
            "size": size,
            "missing": None,
            "extra_player_index": None,
            "players": [],
            "meta": meta
        })

    total_f = _total_f(players)
    remaining = players[:]

    last_two_offpref_count_by_id = last_two_offpref_count_by_id or {}
    last_two_any_offpref_by_id = last_two_any_offpref_by_id or {}

    # Preenchimento por time conforme template (com placeholder para size=5)
    for t_idx, tmpl in enumerate(templates):
        team = teams[t_idx]

        # Se este template é de 5 jogadores, injeta o placeholder de middle e remove 1 middle do template de preenchimento
        tmpl_for_fill = list(tmpl)
        if len(tmpl) == 5:
    # NÃO removemos o middle nem adicionamos placeholder.
    # Apenas marcamos a flag visual para a UI mostrar "Missing — middle".
            team["missing"] = "middle"

        # Duas passagens por slot: pass 1 (restrito) → se vazio, pass 2 (relaxado)
        for pos in tmpl_for_fill:
            # estado de distribuição de F: tentar 1 por time até todos terem uma
            teams_with_f = sum(1 for tm in teams if _team_has_f(tm))
            distributed_f = teams_with_f >= min(T, total_f)

            # ----- PASS 1: restrito
            ranked: List[Tuple[Tuple[int, int, int], Dict]] = []
            for p in remaining:
                r = _rank_for_slot(
                    p, pos,
                    team_has_f_already=_team_has_f(team),
                    distributed_f=distributed_f,
                    last_two_offpref_count_by_id=last_two_offpref_count_by_id,
                    last_two_any_offpref_by_id=last_two_any_offpref_by_id,
                    relaxed=False,
                )
                if r is not None:
                    ranked.append((r, p))

            # ----- PASS 2: relaxado (se necessário)
            if not ranked:
                for p in remaining:
                    r = _rank_for_slot(
                        p, pos,
                        team_has_f_already=_team_has_f(team),
                        distributed_f=distributed_f,
                        last_two_offpref_count_by_id=last_two_offpref_count_by_id,
                        last_two_any_offpref_by_id=last_two_any_offpref_by_id,
                        relaxed=True,
                    )
                    if r is not None:
                        ranked.append((r, p))

            if not ranked:
                # último fallback muito raro: não há ninguém elegível → pula (evitamos travar)
                continue

            # Ordena por (pref_rank asc, fairness_penalty asc, special_penalty asc) + aleatório estável
            ranked.sort(key=lambda t: (t[0][0], t[0][1], t[0][2], random.random()))
            pick = ranked[0][1]

            team["players"].append({
                "name": pick.get("name", ""),
                "pos": pos,
                "gender": _norm(pick.get("gender")),
                "email": pick.get("email", ""),
            })
            remaining.remove(pick)

        # Para times de 7, marcar o índice do 7º jogador (último da lista)
        if team["size"] == 7 and team["players"]:
            team["extra_player_index"] = len(team["players"]) - 1

        # Garantir que times 6/7 tenham missing=None
        if team["size"] in (6, 7) and team.get("missing") is None:
            team["missing"] = None

    # Segurança: se algo sobrar (não deveria, templates já cobrem o N), distribuir como outside
    while remaining:
        # coloca no time com menos jogadores reais (ignorando placeholder)
        idx = min(range(T), key=lambda i: len([pl for pl in teams[i]["players"] if not pl.get("is_missing")]))
        p = remaining.pop(0)
        teams[idx]["players"].append({
            "name": p.get("name", ""),
            "pos": "outside",
            "gender": _norm(p.get("gender")),
            "email": p.get("email", "")
        })
        if teams[idx]["size"] == 7:
            teams[idx]["extra_player_index"] = len(teams[idx]["players"]) - 1

    return teams