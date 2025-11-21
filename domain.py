from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from datetime import date@dataclass


class PlayerPreferences:
    pref1: str
    pref2: str = ""
    pref3: str = ""


# This class is to be used in the future
@dataclass
class PlayerScores:
    
    setter: Optional[int] = None
    hitting: Optional[int] = None
    blocking: Optional[int] = None
    defense: Optional[int] = None
    serve: Optional[int] = None
  

# this is to think better how it can work.
@dataclass
class PlayerConstraints:
    cannot_play_positions: List[str] = field(default_factory=list)
    must_play_with: List[str] = field(default_factory=list)   # list of other player emails or IDs
    cannot_play_with: List[str] = field(default_factory=list) # same idea


@dataclass
class Player:
    name: str
    email: str
    gender: str
    preferences: PlayerPreferences
    scores: Optional[PlayerScores] = None          # future
    constraints: Optional[PlayerConstraints] = None  # per-session rules

    def normalized_gender(self) -> str:
        return (self.gender or "").strip().lower()

@dataclass
class PlayerAssignment:
    player: Player
    position: str
    is_missing: bool = False


@dataclass
class Team:
    id: int
    size: int
    template: List[str]          # e.g. ["setter","middle","middle","outside","outside","outside"]
    players: List[PlayerAssignment] = field(default_factory=list)
    missing: Optional[str] = None        # e.g. "middle"
    extra_player_index: Optional[int] = None
    meta: Dict[str, int] = field(default_factory=dict)

    def has_female(self) -> bool:
        return any(
            pa.player.normalized_gender() == "f" and not pa.is_missing
            for pa in self.players
        )

    def real_player_count(self) -> int:
        return sum(1 for pa in self.players if not pa.is_missing)

@dataclass
class Session:
    session_id: str
    session_date: date
    seed: Optional[int] = None
    players: List[Player] = field(default_factory=list)
    teams: List[Team] = field(default_factory=list)