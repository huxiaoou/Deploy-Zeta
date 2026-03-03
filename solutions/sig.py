import numpy as np
import pandas as pd
from transmatrix import SignalMatrix
from transmatrix.strategy import SignalStrategy
from transmatrix.data_api import create_factor_table
from qtools_sxzq.qdata import CDataDescriptor
from typedef_factor import CCfgFactors


def signalize(data: pd.DataFrame, mid_2_top_ratio: float = 0.5) -> pd.DataFrame:
    rou: float = mid_2_top_ratio**2
    data_rnk = data.rank(pct=True)
    data_neu = data_rnk - data_rnk.mean()
    data_val = rou ** (-data_neu.abs())
    data_sgn = np.sign(data_neu)
    data_sig_r = data_sgn * data_val
    data_sig = data_sig_r / data_sig_r.abs().sum()
    return data_sig


def map_factor_to_signal(nrm_data: pd.DataFrame, factors: list[str]) -> pd.DataFrame:
    """

    :param nrm_data: index = code_name, like "AG9999_SHFE".
                columns = ["avlb", "f1", "f2", ...]
    :param factors:
    :return: sig_data, a pd.Dataframe, index = code_name,
                columns = ["avlb", "f1", "f2", ...]
    """
    data = nrm_data.query("avlb > 0")[factors]
    data_sig = signalize(data)
    res = pd.merge(
        left=nrm_data[["avlb"]],
        right=data_sig,
        left_index=True,
        right_index=True,
        how="left",
    ).fillna(0)
    return res


class CFactorSig(SignalStrategy):
    def __init__(
        self,
        cfg_factors: CCfgFactors,
        data_desc_avlb: CDataDescriptor,
        data_desc_fac_nrm: CDataDescriptor,
    ):
        super().__init__(cfg_factors, data_desc_avlb, data_desc_fac_nrm)

    def init(self):
        self.add_clock(milestones="15:00:00")
        self.subscribe_data("avlb", self.data_desc_avlb.to_args())
        self.subscribe_data("fac_nrm", self.data_desc_fac_nrm.to_args())
        self.create_factor_table(self.cfg_factors.to_list())

    def on_clock(self):
        factors = self.cfg_factors.to_list()
        nrm_data = {"avlb": self.avlb.get_dict("avlb")}
        for factor in factors:
            nrm_data[factor] = self.fac_nrm.get_dict(factor)
        nrm_data = pd.DataFrame(nrm_data)
        sig_data = map_factor_to_signal(nrm_data, factors=factors)
        for factor in factors:
            self.update_factor(factor, sig_data[factor].values)


def main_process_factor_sig(
    span: tuple[str, str],
    codes: list[str],
    cfg_factors: CCfgFactors,
    data_desc_avlb: CDataDescriptor,
    data_desc_fac_nrm: CDataDescriptor,
    dst_db: str,
    table_fac_sig: str,
):
    cfg = {
        "span": span,
        "codes": codes,
        "cache_data": False,
        "progress_bar": True,
    }

    # --- run
    mat = SignalMatrix(cfg)
    factors_sig = CFactorSig(
        cfg_factors=cfg_factors,
        data_desc_avlb=data_desc_avlb,
        data_desc_fac_nrm=data_desc_fac_nrm,
    )
    factors_sig.set_name("factors_sig")
    mat.add_component(factors_sig)
    mat.init()
    mat.run()

    # --- save
    dst_path = f"{dst_db}.{table_fac_sig}"
    create_factor_table(dst_path)
    factors_sig.save_factors(dst_path)
    return 0
