from __future__ import annotations
from typing import List, Dict, Tuple, Optional
import random

VALID_POS = {"setter", "middle", "outside"}

class HistoryFairness:
    """
    Encapsula a informação de fairness das duas últimas datas.
    """
    def __init__(
        self,
        offpref_count_by_id: Optional[Dict[str, int]] = None,
        any_offpref_by_id: Optional[Dict[str, bool]] = None,
    ):
        self.offpref_count_by_id = offpref_count_by_id or {}
        self.any_offpref_by_id = any_offpref_by_id or {}

    def offpref_count(self, email: str) -> int:
        """Quantas vezes o jogador foi off-pref nas duas últimas datas (0..2)."""
        return self.offpref_count_by_id.get(email, 0)

    def has_any_offpref(self, email: str) -> bool:
        """Se o jogador já foi off-pref pelo menos uma vez nas duas últimas datas."""
        return self.any_offpref_by_id.get(email, False)

class TemplatePlanner:
    """
    Responsável por decidir quantos times e a estrutura 7/6/5.
    """

    @staticmethod
    def plan(n_players: int) -> Tuple[int, List[List[str]]]:
        """
        - r=0: T times de 6
        - r=1,2: floor(N/6) times de 6 e r times de 7  → total = floor + r (ordem: 7,7,... depois 6)
        - r=3: ceil times; 3 de 5
        - r=4: ceil times; 2 de 5
        - r=5: ceil times; 1 de 5
        """
        r = n_players % 6
        if r == 0:
            T6 = n_players // 6
            templates = [["setter", "middle", "middle", "outside", "outside", "outside"] for _ in range(T6)]
            return T6, templates

        if r in (1, 2):
            base = n_players // 6
            T = base   # mantemos esse comportamento idêntico ao antigo
            templates_7 = [
                ["setter", "middle", "middle", "outside", "outside", "outside", "outside"]
                for _ in range(r)
            ]
            templates_6 = [
                ["setter", "middle", "middle", "outside", "outside", "outside"]
                for _ in range(T - r)
            ]
            templates = templates_7 + templates_6   # 7 primeiro, depois 6
            return T, templates

        # r in (3, 4, 5) → alguns times de 5 (falta 1 middle)
        T = (n_players + 5) // 6  # ceil
        five = {3: 3, 4: 2, 5: 1}[r]
        # ordem: 6 primeiro, depois 5 (para que 5 sejam os últimos)
        templates_6 = [
            ["setter", "middle", "middle", "outside", "outside", "outside"]
            for _ in range(T - five)
        ]
        templates_5 = [
            ["setter", "middle", "outside", "outside", "outside"]
            for _ in range(five)
        ]  # 5: {1S,1M,3O}
        templates = templates_6 + templates_5
        return T, templates

class SlotRanker:
    """
    Responsável por decidir o ranking de um jogador para um slot específico.
    Usa o histórico de fairness via HistoryFairness
    e a lista de jogadores protegidos (keep pref1).
    """

    def __init__(self, history: HistoryFairness, keep_pref_emails: Optional[set[str]] = None):
        self.history = history
        # conjunto de e-mails protegidos: queremos evitar tirar esses jogadores da pref1
        self.keep_pref_emails = {(e or "").strip().lower() for e in (keep_pref_emails or set())}

    @staticmethod
    def _norm(s: Optional[str]) -> str:
        return (s or "").strip().lower()

    def rank_for_slot(
        self,
        player: Dict,
        pos: str,
        *,
        team_has_f_already: bool,
        distributed_f: bool,
        relaxed: bool,
    ) -> Optional[Tuple[int, int, int]]:
        email = self._norm(player.get("email"))
        gender = self._norm(player.get("gender"))
        p1 = self._norm(player.get("pref1"))
        p2 = self._norm(player.get("pref2"))
        p3 = self._norm(player.get("pref3"))

        # Jogador "protegido": marcado na página de controle
        # (via regra de cannot_play_positions → interpretada como keep pref1)
        protected = (email in self.keep_pref_emails) and (p1 in VALID_POS)

        # Regra principal do keep pref1:
        # se é protegido, NÃO usar esse jogador fora da pref1
        # na fase normal (relaxed=False). Só considerar fora da pref1
        # em modo relaxado, quando não há mais alternativas.
        if protected and not relaxed and pos != p1:
            return None

        # Quantas vezes essa pessoa já foi off-pref nas duas últimas datas
        off_ct = self.history.offpref_count(email)

        all_setter = (p1, p2, p3) == ("setter", "setter", "setter")

                # ---------- BLOCO ESPECÍFICO PARA MIDDLE ----------
        if pos == "middle":
            # 1) F só joga middle se pref1 == "middle"
            if gender == "f" and p1 != "middle":
                return None

            # 2) Se já foi off-pref nas DUAS últimas datas, não sacrificar de novo
            if not relaxed and off_ct >= 2:
                return None

            # 3) Verdadeiros middles têm prioridade máxima
            if p1 == "middle":
                pref_rank = 1
                going_off = False
            else:
                # 4) BACKFILL: aqui vamos diferenciar QUEM é mais "voluntário" a ser middle

                going_off = True  # está indo fora da pref1

                # Queremos uma escala de "vontade" de ser middle:
                # número MENOR = mais disposto a ser middle
                # número MAIOR = vai para o final da fila de backfill

                # Caso 4.1 – middle em segunda preferência: ok usar como backfill
                # exemplos: setter, middle, outside  ou  outside, middle, setter
                if p2 == "middle":
                    middle_willingness = 2

                # Caso 4.2 – nunca pediu middle em nenhuma preferência
                elif "middle" not in (p1, p2, p3):
                    # nunca pediu middle → bem no final da fila
                    middle_willingness = 6

                # Caso 4.3 – middle só em 3ª preferência E as duas primeiras são setter/outside
                # exemplos: setter, outside, middle  ou  outside, setter, middle
                elif (p3 == "middle") and (p1 in {"setter", "outside"}) and (p2 in {"setter", "outside"}):
                    # essa pessoa pediu claramente jogar como setter/outside
                    # e só aceitou middle como último recurso → final da fila de backfill
                    middle_willingness = 6

                # Caso 4.4 – outros casos "neutros"
                else:
                    # Ex.: alguém com combinações mais estranhas onde middle não está tão rejeitado
                    middle_willingness = 4

                pref_rank = middle_willingness

            # 5) Fairness penalty: maior para quem já foi off-pref antes
            fairness_penalty = 0
            if going_off:
                fairness_penalty += off_ct * 2
                if off_ct >= 1 and not relaxed:
                    fairness_penalty += 3

            special_penalty = 0
            return (pref_rank, fairness_penalty, special_penalty)

        # ---------- OUTRAS POSIÇÕES (setter / outside) ----------

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

class TeamGenerator:
    def __init__(
        self,
        players: List[Dict],
        *,
        seed: Optional[int] = None,
        last_two_offpref_count_by_id: Optional[Dict[str, int]] = None,
        last_two_any_offpref_by_id: Optional[Dict[str, bool]] = None,
        keep_pref_emails: Optional[set[str]] = None,
    ):
        self.players = players[:]
        self.seed = seed

        # componentes
        self.history = HistoryFairness(
            offpref_count_by_id=last_two_offpref_count_by_id,
            any_offpref_by_id=last_two_any_offpref_by_id,
        )
        self.slot_ranker = SlotRanker(self.history, keep_pref_emails=keep_pref_emails)

        self.teams: List[Dict] = []
        self.templates: List[List[str]] = []

        if self.seed is not None:
            random.seed(self.seed)
        random.shuffle(self.players)


class TeamGenerator:
    def __init__(
        self,
        players: List[Dict],
        *,
        seed: Optional[int] = None,
        last_two_offpref_count_by_id: Optional[Dict[str, int]] = None,
        last_two_any_offpref_by_id: Optional[Dict[str, bool]] = None,
        keep_pref_emails: Optional[set[str]] = None,
    ):
        self.players = players[:]
        self.seed = seed

        # componentes
        self.history = HistoryFairness(
            offpref_count_by_id=last_two_offpref_count_by_id,
            any_offpref_by_id=last_two_any_offpref_by_id,
        )
        self.slot_ranker = SlotRanker(self.history, keep_pref_emails=keep_pref_emails)

        self.teams: List[Dict] = []
        self.templates: List[List[str]] = []

        if self.seed is not None:
            random.seed(self.seed)
        random.shuffle(self.players)

    @staticmethod
    def _norm(s: Optional[str]) -> str:
        return (s or "").strip().lower()

    @staticmethod
    def _total_f(players: List[Dict]) -> int:
        return sum(1 for p in players if (p.get("gender") or "").lower() == "f")

    @staticmethod
    def _team_has_f(team: Dict) -> bool:
        return any(
            (pl.get("gender") or "").lower() == "f"
            for pl in team.get("players", [])
            if not pl.get("is_missing")
        )

    def generate(self) -> List[Dict]:
        if not self.players:
            return []

        n = len(self.players)
        T, templates = TemplatePlanner.plan(n)
        self.templates = templates

        teams: List[Dict] = []
        for i, tmpl in enumerate(templates):
            size = len(tmpl)
            meta = {
                "setter": 1,
                "middle": 2 if size in (6, 7) else 1,
                "outside": 4 if size == 7 else 3,
            }
            teams.append({
                "team": i + 1,
                "size": size,
                "missing": None,
                "extra_player_index": None,
                "players": [],
                "meta": meta,
            })

        total_f = self._total_f(self.players)
        remaining = self.players[:]

        T_count = len(teams)

        for t_idx, tmpl in enumerate(templates):
            team = teams[t_idx]

            tmpl_for_fill = list(tmpl)
            if len(tmpl) == 5:
                team["missing"] = "middle"

            for pos in tmpl_for_fill:
                teams_with_f = sum(1 for tm in teams if self._team_has_f(tm))
                distributed_f = teams_with_f >= min(T_count, total_f)

                ranked: List[Tuple[Tuple[int, int, int], Dict]] = []
                for p in remaining:
                    r = self.slot_ranker.rank_for_slot(
                        p, pos,
                        team_has_f_already=self._team_has_f(team),
                        distributed_f=distributed_f,
                        relaxed=False,
                    )
                    if r is not None:
                        ranked.append((r, p))

                if not ranked:
                    for p in remaining:
                        r = self.slot_ranker.rank_for_slot(
                            p, pos,
                            team_has_f_already=self._team_has_f(team),
                            distributed_f=distributed_f,
                            relaxed=True,
                        )
                        if r is not None:
                            ranked.append((r, p))

                if not ranked:
                    continue

                ranked.sort(key=lambda t: (t[0][0], t[0][1], t[0][2], random.random()))
                pick = ranked[0][1]

                team["players"].append({
                    "name": pick.get("name", ""),
                    "pos": pos,
                    "gender": self._norm(pick.get("gender")),
                    "email": pick.get("email", ""),
                })
                remaining.remove(pick)

            if team["size"] == 7 and team["players"]:
                team["extra_player_index"] = len(team["players"]) - 1

            if team["size"] in (6, 7) and team.get("missing") is None:
                team["missing"] = None

        while remaining:
            idx = min(
                range(T_count),
                key=lambda i: len([pl for pl in teams[i]["players"] if not pl.get("is_missing")])
            )
            p = remaining.pop(0)
            teams[idx]["players"].append({
                "name": p.get("name", ""),
                "pos": "outside",
                "gender": self._norm(p.get("gender")),
                "email": p.get("email", ""),
            })
            if teams[idx]["size"] == 7:
                teams[idx]["extra_player_index"] = len(teams[idx]["players"]) - 1

        self.teams = teams
        return teams

def generate_teams(
    players: List[Dict],
    *,
    seed: Optional[int] = None,
    last_two_offpref_count_by_id: Optional[Dict[str, int]] = None,
    last_two_any_offpref_by_id: Optional[Dict[str, bool]] = None,
    keep_pref_emails: Optional[set[str]] = None,
) -> List[Dict]:
    generator = TeamGenerator(
        players,
        seed=seed,
        last_two_offpref_count_by_id=last_two_offpref_count_by_id,
        last_two_any_offpref_by_id=last_two_any_offpref_by_id,
        keep_pref_emails=keep_pref_emails,
    )
    return generator.generate()

def _norm_email(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def postprocess_teams(
    *,
    teams: List[Dict],
    session_rules: List[Dict],
    last_two_offpref_count_by_id: Optional[Dict[str, int]] = None,
    last_two_any_offpref_by_id: Optional[Dict[str, bool]] = None,
) -> List[Dict]:
    """
    Aplica regras suaves (soft control) DEPOIS da geração principal dos times.

    Regras vêm do Google Sheets, no formato:
      {
        "player_email": "a@x.com",
        "cannot_play_positions": ["middle", ...],
        "must_play_with": ["b@x.com", ...],
        "cannot_play_with": ["c@x.com", ...],
      }

    Comportamento (best-effort):
      1) Tenta corrigir cannot_play_positions com trocas de posição dentro do mesmo time.
      2) Tenta atender must_play_with trocando jogadores entre times (mesma posição).
      3) Tenta separar cannot_play_with trocando jogadores entre times (mesma posição).

    Se não achar swap seguro, ignora aquela regra e segue.
    """
    if not teams:
        return teams

    offpref_count = last_two_offpref_count_by_id or {}

    # -------- Normaliza regras em um mapa: email -> estrutura de regras --------
    rules_by_email: Dict[str, Dict] = {}

    for r in session_rules or []:
        email = _norm_email(r.get("player_email"))
        if not email:
            continue
        entry = rules_by_email.setdefault(
            email,
            {
                "player_email": email,
                "cannot_play_positions": set(),
                "must_play_with": set(),
                "cannot_play_with": set(),
            },
        )

        for pos in r.get("cannot_play_positions") or []:
            p = (pos or "").strip().lower()
            if p in VALID_POS:
                entry["cannot_play_positions"].add(p)

        for em in r.get("must_play_with") or []:
            e2 = _norm_email(em)
            if e2:
                entry["must_play_with"].add(e2)

        for em in r.get("cannot_play_with") or []:
            e2 = _norm_email(em)
            if e2:
                entry["cannot_play_with"].add(e2)

    # Se não há regras, nada a fazer.
    if not rules_by_email:
        return teams

    # -------- Helper: índice email -> (team_idx, player_idx) --------
    def build_index() -> Dict[str, Tuple[int, int]]:
        idx: Dict[str, Tuple[int, int]] = {}
        for ti, team in enumerate(teams):
            for pi, p in enumerate(team.get("players", [])):
                if p.get("is_missing"):
                    continue
                em = _norm_email(p.get("email"))
                if em:
                    idx[em] = (ti, pi)
        return idx

    email_index = build_index()

    # ===============================================================
    # 1) cannot_play_positions → trocas de posição dentro do time
    # ===============================================================
    for email, rule in rules_by_email.items():
        forbidden = rule["cannot_play_positions"]
        if not forbidden:
            continue

        loc = email_index.get(email)
        if not loc:
            continue  # jogador não está em nenhum time (não achou no CSV ou não foi escalado)

        ti, pi = loc
        player = teams[ti]["players"][pi]
        current_pos = (player.get("pos") or "").strip().lower()
        if current_pos not in forbidden:
            continue  # já está numa posição permitida

        # Vamos tentar trocar de posição com alguém do MESMO time:
        #   - que tenha outra posição
        #   - que não tenha o current_pos proibido
        #   - preferindo quem tem histórico de off-pref menor
        #   - se a posição for "middle", tentar evitar colocar F em middle se houver alternativa
        candidates = []
        for cj, cp in enumerate(teams[ti].get("players", [])):
            if cj == pi:
                continue
            if cp.get("is_missing"):
                continue

            other_pos = (cp.get("pos") or "").strip().lower()
            if other_pos == current_pos:
                # trocar "middle" com "middle" não resolve o problema
                continue

            c_email = _norm_email(cp.get("email"))
            c_rule = rules_by_email.get(c_email)
            if c_rule and current_pos in c_rule["cannot_play_positions"]:
                # o colega também não pode jogar nessa posição
                continue

            # Fairness: evitar quem já foi muito sacrificado
            c_off = offpref_count.get(c_email, 0)

            c_gender = (cp.get("gender") or "").strip().lower()
            candidates.append((cj, c_off, c_gender, other_pos))

        if not candidates:
            # não há ninguém viável para troca dentro do time
            continue

        # Escolher melhor candidato
        if current_pos == "middle":
            # Preferir não-feminino, depois menor off-pref
            candidates.sort(key=lambda t: (1 if t[2] == "f" else 0, t[1]))
        else:
            # Só minimizar off-pref
            candidates.sort(key=lambda t: t[1])

        best_idx, _, _, _ = candidates[0]
        teammate = teams[ti]["players"][best_idx]

        # Troca apenas as POSIÇÕES, mantendo os jogadores no mesmo time
        player_pos_before = player.get("pos")
        teammate_pos_before = teammate.get("pos")
        player["pos"], teammate["pos"] = teammate_pos_before, player_pos_before
        # índices continuam corretos (mesmo team, mesmo índice)

    # Recria índice após as mudanças de posição
    email_index = build_index()

    # ===============================================================
    # 2) must_play_with → trocas entre times (mesma posição)
    # ===============================================================
    for email, rule in rules_by_email.items():
        must_with = rule["must_play_with"]
        if not must_with:
            continue

        loc_a = email_index.get(email)
        if not loc_a:
            continue

        team_a_idx, player_a_idx = loc_a

        for other_email in must_with:
            loc_b = email_index.get(other_email)
            if not loc_b:
                continue

            team_b_idx, player_b_idx = loc_b

            if team_a_idx == team_b_idx:
                # já estão juntos
                continue

            player_b = teams[team_b_idx]["players"][player_b_idx]
            b_pos = (player_b.get("pos") or "").strip().lower()

            # Tentar trazer B para o time A, trocando com alguém de MESMA posição
            candidates = []
            for cj, cp in enumerate(teams[team_a_idx].get("players", [])):
                if cp.get("is_missing"):
                    continue
                if cj == player_a_idx:
                    # aqui evitamos mexer no próprio A
                    continue
                if (cp.get("pos") or "").strip().lower() != b_pos:
                    continue

                c_email = _norm_email(cp.get("email"))
                c_off = offpref_count.get(c_email, 0)
                candidates.append((cj, c_off))

            if not candidates:
                # não achou ninguém no time A com mesma posição para trocar
                continue

            candidates.sort(key=lambda t: t[1])
            swap_idx, _ = candidates[0]

            # Faz swap entre times: B <-> jogador de mesma posição do time A
            teams[team_a_idx]["players"][swap_idx], teams[team_b_idx]["players"][player_b_idx] = (
                teams[team_b_idx]["players"][player_b_idx],
                teams[team_a_idx]["players"][swap_idx],
            )

            # Recria índice pois times mudaram
            email_index = build_index()

    # ===============================================================
    # 3) cannot_play_with → separar pares via swap entre times
    # ===============================================================
    for email, rule in rules_by_email.items():
        cannot_with = rule["cannot_play_with"]
        if not cannot_with:
            continue

        loc_a = email_index.get(email)
        if not loc_a:
            continue

        team_a_idx, player_a_idx = loc_a

        for other_email in cannot_with:
            loc_b = email_index.get(other_email)
            if not loc_b:
                continue

            team_b_idx, player_b_idx = loc_b

            if team_a_idx != team_b_idx:
                # já estão separados
                continue

            # Tentamos mover B para outro time, trocando com alguém da MESMA posição
            player_b = teams[team_b_idx]["players"][player_b_idx]
            b_pos = (player_b.get("pos") or "").strip().lower()

            swapped = False
            for target_team_idx, team in enumerate(teams):
                if target_team_idx == team_b_idx:
                    continue

                candidates = []
                for cj, cp in enumerate(team.get("players", [])):
                    if cp.get("is_missing"):
                        continue
                    if (cp.get("pos") or "").strip().lower() != b_pos:
                        continue

                    c_email = _norm_email(cp.get("email"))
                    c_off = offpref_count.get(c_email, 0)
                    candidates.append((cj, c_off))

                if not candidates:
                    continue

                candidates.sort(key=lambda t: t[1])
                swap_idx, _ = candidates[0]

                # swap entre time_b_idx e target_team_idx
                teams[target_team_idx]["players"][swap_idx], teams[team_b_idx]["players"][player_b_idx] = (
                    teams[team_b_idx]["players"][player_b_idx],
                    teams[target_team_idx]["players"][swap_idx],
                )

                swapped = True
                email_index = build_index()
                break  # para no primeiro swap que der certo

            # se não conseguiu mover B para lugar nenhum, deixa como está

    return teams