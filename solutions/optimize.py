import numpy as np
import pandas as pd
from typing import Literal
from transmatrix import SignalMatrix
from transmatrix.strategy import SignalStrategy
from transmatrix.data_api import create_factor_table
from transmatrix.event.scheduler import PeriodScheduler
from qtools_sxzq.qdata import CDataDescriptor
from typedef import CCfgOptimizer

# ----------------------------- FOR FACTORS -----------------------------


class COptimizerFacWgt(SignalStrategy):
    CONST_SAFE_RET_LENGTH = 10
    CONST_ANNUAL_FAC = 250

    def __init__(
        self,
        factors: list[str],
        tgt_rets: list[str],
        cfg_optimizer_fac: CCfgOptimizer,
        data_desc_sim: CDataDescriptor,
    ):
        self.factors: list[str]
        self.cfg_optimizer_fac: CCfgOptimizer
        self.data_desc_sim: CDataDescriptor
        super().__init__(
            factors,
            tgt_rets,
            cfg_optimizer_fac,
            data_desc_sim,
        )
        p = len(self.factors)
        self.opt_val: dict[str, pd.Series] = {
            tgt_ret: pd.Series(np.ones(p) / p, index=self.factors) for tgt_ret in self.tgt_rets
        }

    def init(self):
        # on every day
        self.add_scheduler(milestones="15:00:00", handler=self.on_day_end)

        # on optimizing date
        scheduler = PeriodScheduler(periods="W", milestone="16:00:00")
        self.add_scheduler(scheduler=scheduler, handler=self.on_optimize_date_end)

        # subscribe data
        self.subscribe_data("sim_data", self.data_desc_sim.to_args())

        # create factor tables to record factor
        self.create_factor_table(self.tgt_rets)

    def on_day_end(self):
        for tgt_ret in self.tgt_rets:
            self.update_factor(tgt_ret, self.opt_val[tgt_ret])

    def on_optimize_date_end(self):
        for tgt_ret in self.tgt_rets:
            slc_codes = [f"{fac}-{tgt_ret}" for fac in self.factors]
            rename_mapper = {k: v for k, v in zip(slc_codes, self.factors)}
            net_ret_data: pd.DataFrame = self.sim_data.get_window_df(
                field="net_ret",
                length=self.cfg_optimizer_fac.window,
                codes=slc_codes,
            ).rename(columns=rename_mapper)
            opt_val = self.core(ret_data=net_ret_data, method="eq")
            default_val = pd.Series({k: 0 for k in self.factors})
            default_val.update(opt_val)
            self.opt_val[tgt_ret] = default_val

    def core(self, ret_data: pd.DataFrame, method: Literal["eq"]) -> pd.Series:
        if method == "eq":
            p = ret_data.shape[1]
            return pd.Series(np.ones(p) / p, index=self.factors)
        else:
            raise ValueError(f"Invalid method = {method}")


def main_process_optimize_fac_wgt(
    span: tuple[str, str],
    factors: list[str],
    tgt_rets: list[str],
    cfg_optimizer_fac: CCfgOptimizer,
    data_desc_sim: CDataDescriptor,
    dst_db: str,
    table_optimize_fac: str,
):
    """

    :param span:
    :param factors:
    :param tgt_rets: ["opn", "cls"]
    :param cfg_optimizer_fac:
    :param data_desc_sim:
    :param dst_db: database to save optimized weights for factors
    :param table_optimize_fac: table to save optimized weights for factors
    :return:
    """
    cfg = {
        "span": span,
        "codes": factors,
        "cache_data": False,
        "progress_bar": True,
    }

    # --- run
    mat = SignalMatrix(cfg)
    optimizer = COptimizerFacWgt(
        factors=factors,
        tgt_rets=tgt_rets,
        cfg_optimizer_fac=cfg_optimizer_fac,
        data_desc_sim=data_desc_sim,
    )
    optimizer.set_name("optimizer")
    mat.add_component(optimizer)
    mat.init()
    mat.run()

    # --- save
    dst_path = f"{dst_db}.{table_optimize_fac}"
    create_factor_table(dst_path)
    optimizer.save_factors(dst_path)
    return 0
