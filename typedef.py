from itertools import product
from dataclasses import dataclass
from typing import Literal
from typedef_factor import CCfgFactors


@dataclass(frozen=True)
class CCfgInstru:
    sectorL0: Literal["C", "E"]  # C = commodity, E = Equity
    sectorL1: Literal["AUG", "MTL", "BLK", "OIL", "CHM", "AGR"]


TInstruName = str
TUniverse = dict[TInstruName, CCfgInstru]
TSectors = list[str]

@dataclass(frozen=True)
class CCfgProj:
    pid: str
    vid: str
    project_data_dir: str
    path_calendar: str
    codes: list[str]
    factors: CCfgFactors