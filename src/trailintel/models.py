from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True)
class AthleteRecord:
    input_name: str
    utmb_index: Optional[float] = None
    utmb_match_name: Optional[str] = None
    utmb_match_score: Optional[float] = None
    utmb_profile_url: Optional[str] = None
    itra_score: Optional[float] = None
    itra_match_name: Optional[str] = None
    itra_match_score: Optional[float] = None
    itra_profile_url: Optional[str] = None
    betrail_score: Optional[float] = None
    betrail_match_name: Optional[str] = None
    betrail_match_score: Optional[float] = None
    betrail_profile_url: Optional[str] = None
    notes: str = ""

    @property
    def combined_score(self) -> float:
        if self.utmb_index is not None and self.itra_score is not None:
            return (self.utmb_index * 0.6) + (self.itra_score * 0.4)
        if self.utmb_index is not None:
            return self.utmb_index
        if self.itra_score is not None:
            return self.itra_score
        if self.betrail_score is not None:
            return self.betrail_score * 10.0
        return 0.0
