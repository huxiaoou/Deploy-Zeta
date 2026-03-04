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
class CCfgQSim:
    win: int


@dataclass(frozen=True)
class CCfgCSim:
    init_cash: float
    oi_cap_ratio: float
    cost_rate_sub: float
    cost_rate_pri: float


@dataclass(frozen=True)
class CCfgProj:
    pid: str
    vid: str
    project_data_dir: str
    path_calendar: str
    codes: list[str]
    factors: CCfgFactors
    qsim: CCfgQSim
    csim: CCfgCSim
    tgt_rets: list[str]


@dataclass(frozen=True)
class CCfgTables:
    fac_raw: str
    fac_nrm: str
    fac_sig: str
    fac_ewa: str
    sim_fac: str
    optimize_fac: str
    sig_stg: str


@dataclass(frozen=True)
class CCfgDbs:
    public: str
    basic: str
    user: str


@dataclass(frozen=True)
class CSimArgs:
    sig: str
    ret: str

    @property
    def save_id(self) -> str:
        return f"{self.sig}-{self.ret}"
