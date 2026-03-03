import datetime as dt
import numpy as np
import pandas as pd
import os
from dataclasses import dataclass
from typing import Any, Union


@dataclass
class CDecay:
    rate: float
    win: int

    def __post_init__(self):
        rou = np.power(self.rate, 1 / (self.win - 1)) if self.win > 1 else 1.0
        wgt = np.power(rou, np.arange(self.win, 0, -1))
        self.wgt = wgt / wgt.sum()

    def __str__(self) -> str:
        return f"CDecayR{int(self.rate * 10):02d}W{self.win:02d}"


@dataclass(frozen=True)
class CCfgFactor:
    name: str
    args: Any
    decay: CDecay
    risk: bool

    @property
    def lag(self) -> int:
        raise NotImplementedError


TInterFactorData = dict[str, Union[dict[str, pd.Series], pd.DataFrame]]


class CAlgFactor:
    def __init__(self, cfg: CCfgFactor):
        self.cfg = cfg

    def __repr__(self):
        return f"CAlgFactor({self.cfg.name})"

    @staticmethod
    def add_to_factor(factor_data: TInterFactorData, var_name: str, code: str, val: pd.Series):
        if var_name not in factor_data:
            factor_data[var_name] = {}
        factor_data[var_name][code] = val

    @staticmethod
    def parse_index_to_datetime(s: pd.Series, time_string: str = "15:00:00", fmt: str = "%Y-%m-%d %H:%M:%S"):
        s.index = s.index.map(lambda z: dt.datetime.strptime(f"{z} {time_string}", fmt))

    def update_factor_data_in_pre_trans_form(
        self,
        code: str,
        factor_data: TInterFactorData,
        instru_data_1d: pd.DataFrame,
        instru_data_1m: pd.DataFrame,
    ):
        pass

    def cal_factor(self, *args, **kwargs) -> pd.Series:
        raise NotImplementedError


TMgrCfgAlg = dict[str, tuple[CCfgFactor, CAlgFactor]]


class CCfgFactors:
    def __init__(self, algs_dir: str, cfg_data: dict[str, dict], default_decay: dict):
        self.mgr: TMgrCfgAlg = {}
        for module in sorted(os.listdir(algs_dir)):
            if module.endswith(".py"):
                factor = module.split(".")[0]  # exclude ".py"
                module_path = f"{algs_dir}.{factor}"
                module_contents = __import__(module_path)
                type_cfg: type[CCfgFactor] = getattr(module_contents.__dict__[factor], f"CCfgFactor{factor.upper()}")
                type_alg: type[CAlgFactor] = getattr(module_contents.__dict__[factor], f"CAlgFactor{factor.upper()}")
                v = cfg_data[factor]
                cfg = type_cfg(
                    name=factor,
                    args=v["args"],
                    decay=CDecay(**v.get("decay", default_decay)),
                    risk=v.get("risk", False),
                )
                alg = type_alg(cfg=cfg)
                self.mgr[factor] = (cfg, alg)

    @property
    def lag(self) -> int:
        d = max([cfg.lag for _, (cfg, __) in self.mgr.items()])
        return max(30, d) * 2

    def to_list(self) -> list[str]:
        return list(self.mgr)

    def get_risk_factors(self) -> list[str]:
        return [factor for factor, (cfg, _) in self.mgr.items() if cfg.risk]

    def get_alpha_factors(self) -> list[str]:
        return [factor for factor, (cfg, _) in self.mgr.items() if not cfg.risk]

    def display(self):
        i = 0
        for factor, (cfg, alg) in self.mgr.items():
            print(f"{i:>02d} |{factor:>12s} = {cfg}, {alg}")
            i += 1

    def get_factor_cfg(self, factor: str) -> CCfgFactor:
        return self.mgr[factor][0]

    def get_factors_wgts(self) -> dict[str, np.ndarray]:
        res: dict[str, np.ndarray] = {}
        for factor, (cfg, alg) in self.mgr.items():
            res[factor] = cfg.decay.wgt
        return res
