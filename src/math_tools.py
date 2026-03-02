import numpy as np
import pandas as pd
from typing import Union, Literal


# --------------- Algs for factors ---------------


def gen_exp_wgt(k: int, rate: float = 0.30) -> np.ndarray:
    k0, d = k // 2, k % 2
    rou = np.power(rate, 1 / (k0 - 1)) if k0 > 1 else 1
    sgn = np.array([1] * k0 + [0] * d + [-1] * k0)
    val = np.power(rou, list(range(k0)) + [k0] * d + list(range(k0 - 1, -1, -1)))
    s = sgn * val
    abs_sum = np.abs(s).sum()
    wgt = (s / abs_sum) if abs_sum > 0 else np.zeros(k)
    return wgt


def weighted_mean(x: Union[pd.Series, pd.DataFrame], wgt: pd.Series = None) -> float:
    if wgt is None:
        return x.mean()
    else:
        w = wgt / wgt.abs().sum()
        return w @ x


def weighted_volatility(x: pd.Series, wgt: pd.Series = None) -> float:
    if wgt is None:
        return x.std()
    else:
        w = wgt / wgt.abs().sum()
        mu = x @ w
        x2 = (x**2) @ w
        return np.sqrt(x2 - mu**2)


def robust_div(
    x: Union[pd.Series, pd.DataFrame],
    y: Union[pd.Series, pd.DataFrame],
    nan_val: float = np.nan,
) -> Union[pd.Series, pd.DataFrame]:
    """

    :param x: must have the same shape as y
    :param y:
    :param nan_val:
    :return:
    """

    return (x / y.where(y != 0, np.nan)).fillna(nan_val)  # type:ignore


def robust_ret(
    x: pd.Series,
    y: pd.Series,
    scale: float = 1.0,
    condition: Literal["ne", "ge", "le"] = "ne",
) -> pd.Series:
    """

    :param x: must have the same length as y
    :param y:
    :param scale: return scale
    :param condition:
    :return:
    """
    if condition == "ne":
        return (x / y.where(y != 0, np.nan) - 1) * scale
    elif condition == "ge":
        return (x / y.where(y > 0, np.nan) - 1) * scale
    elif condition == "le":
        return (x / y.where(y < 0, np.nan) - 1) * scale
    else:
        raise ValueError("parameter condition must be 'ne', 'ge', or 'le'.")


def cal_top_corr(x: pd.DataFrame, y: pd.DataFrame, sort_var: pd.DataFrame, top_size: int, ascending: bool = False):
    res = {}
    for code in x.columns:
        df = pd.DataFrame(
            {
                "x": x[code],
                "y": y[code],
                "sv": sort_var[code],
            }
        )
        sorted_data = df.sort_values(by="sv", ascending=ascending)
        top_data = sorted_data.head(top_size)
        res[code] = top_data[["x", "y"]].astype(np.float64).corr(method="spearman").at["x", "y"]
    return pd.Series(res)


def cal_res(y: pd.DataFrame, x: pd.DataFrame) -> pd.Series:
    xyb = (y * x).mean()
    xxb = (x * x).mean()
    xb, yb = x.mean(), y.mean()
    icov = xyb - xb * yb
    ivar = xxb - xb * xb
    beta = robust_div(icov, ivar)
    return y.iloc[-1, :] - beta * x.iloc[-1, :]  # type:ignore


# --------------- Algs for factors ---------------


def cal_roll_return(x: pd.Series, ticker_n: str, ticker_d: str, prc_n: str, prc_d: str):
    if x.isnull().any():
        return np.nan
    if x[prc_d] > 0:
        cntrct_d, cntrct_n = x[ticker_d].split("_")[0], x[ticker_n].split("_")[0]
        month_d, month_n = int(cntrct_d[-2:]), int(cntrct_n[-2:])
        dlt_month = (month_d - month_n) % 12
        if dlt_month > 0:
            return np.round((x[prc_n] / x[prc_d] - 1) / dlt_month * 12 * 100, 6)
        else:
            return np.nan
    else:
        return np.nan


def cal_reoc_by_minute(tday_minb_data: pd.DataFrame, eff: str = "eff", ret: str = "ret") -> float:
    net_data = tday_minb_data.iloc[1:, :]
    eff_sum = net_data[eff].sum()
    if eff_sum > 0:
        wgt = net_data[eff] / eff_sum
        reoc = net_data[ret].fillna(0) @ wgt * 1e4
        return reoc
    else:
        return 0.0


def cal_reoc(reoc: pd.DataFrame, w0: int, w1: int) -> pd.Series:
    m0, m1 = reoc.tail(w0).sum(), reoc.tail(w1).sum()
    return m0 * np.sqrt(w1 / w0) - m1
