import pandas as pd
import numpy as np
from transmatrix import SignalMatrix
from transmatrix.strategy import SignalStrategy
from transmatrix.data_api import create_factor_table
from qtools_sxzq.qdata import CDataDescriptor
from typedef_factor import CCfgFactors


class CFactorEwa(SignalStrategy):
    def __init__(
        self,
        cfg_factors: CCfgFactors,
        data_desc_fac_sig: CDataDescriptor,
    ):
        super().__init__(cfg_factors, data_desc_fac_sig)
        self.wgts: dict[str, np.ndarray] = cfg_factors.get_factors_wgts()

    def init(self):
        self.add_clock(milestones="15:00:00")
        self.subscribe_data("fac_sig", self.data_desc_fac_sig.to_args())
        self.create_factor_table(self.cfg_factors.to_list())

    def on_clock(self):
        factors = self.cfg_factors.to_list()
        ewa_data = {}
        for factor in factors:
            sig_data = self.fac_sig.get_window_df(factor, 5)
            wgt = self.wgts[factor]
            ewa_data[factor] = wgt @ sig_data.fillna(0)
        ewa_data_r = pd.DataFrame(ewa_data).fillna(0)
        ewa_data = ewa_data_r / ewa_data_r.abs().sum()
        for factor in factors:
            self.update_factor(factor, ewa_data[factor].values)


def main_process_factor_ewa(
    span: tuple[str, str],
    codes: list[str],
    cfg_factors: CCfgFactors,
    data_desc_fac_sig: CDataDescriptor,
    dst_db: str,
    table_fac_ewa: str,
):
    cfg = {
        "span": span,
        "codes": codes,
        "cache_data": False,
        "progress_bar": True,
    }

    # --- run
    mat = SignalMatrix(cfg)
    factors_sig = CFactorEwa(
        cfg_factors=cfg_factors,
        data_desc_fac_sig=data_desc_fac_sig,
    )
    factors_sig.set_name("factors_sig")
    mat.add_component(factors_sig)
    mat.init()
    mat.run()

    # --- save
    dst_path = f"{dst_db}.{table_fac_ewa}"
    create_factor_table(dst_path)
    factors_sig.save_factors(dst_path)
    return 0
