import yaml
from qtools_sxzq.qwidgets import check_and_mkdir
from qtools_sxzq.qdata import CDataDescriptor, CMarketDescriptor
from typedef import TUniverse, CCfgInstru, TSectors
from typedef_factor import CCfgFactors
from typedef import CCfgProj, CCfgTables, CCfgDbs, CCfgCSim


with open("config.yaml", "r") as f:
    _config = yaml.safe_load(f)

universe: TUniverse = {k: CCfgInstru(**v) for k, v in _config["universe"].items()}
universe_sector: dict[str, str] = {k: v.sectorL1 for k, v in universe.items()}
sectors: TSectors = sorted(list(set([v.sectorL1 for v in universe.values()])))

cfg_factors = CCfgFactors(algs_dir="factor_algs", cfg_data=_config["factors"])

cfg = CCfgProj(
    pid=_config["project_id"],
    vid=_config["version_id"],
    project_data_dir=_config["project_data_dir"],
    path_calendar=_config["path_calendar"],
    codes=list(universe),
    factors=cfg_factors,
    csim=CCfgCSim(**_config["csim"]),
)
check_and_mkdir(cfg.project_data_dir)

cfg_tables = CCfgTables(
    fac_raw=f"{cfg.pid}_tbl_fac_raw_{cfg.vid}",
    fac_nrm=f"{cfg.pid}_tbl_fac_nrm_{cfg.vid}",
    fac_sig=f"{cfg.pid}_tbl_fac_sig_{cfg.vid}",
    fac_ewa=f"{cfg.pid}_tbl_fac_ewa_{cfg.vid}",
    sim_fac=f"{cfg.pid}_tbl_sim_fac_{cfg.vid}",
    optimize_fac=f"{cfg.pid}_tbl_optimize_fac_{cfg.vid}",
    sig_stg=f"{cfg.pid}_tbl_sig_stg_{cfg.vid}",
)

cfg_dbs = CCfgDbs(**_config["dbs"])

"""
-------------------
--- public data ---
-------------------
"""
data_desc_preprocess = CDataDescriptor(
    codes=cfg.codes,
    db_name=cfg_dbs.user,
    **_config["src_tables"]["preprocess"],
)
data_desc_dominant = CDataDescriptor(
    codes=cfg.codes,
    db_name=cfg_dbs.user,
    **_config["src_tables"]["dominant"],
)
data_desc_avlb = CDataDescriptor(
    codes=cfg.codes,
    db_name=cfg_dbs.user,
    **_config["src_tables"]["avlb"],
)
data_desc_icov = CDataDescriptor(
    codes=cfg.codes,
    db_name=cfg_dbs.user,
    fields=[_.lower() for _ in cfg.codes],
    **_config["src_tables"]["icov"],
)
data_desc_css = CDataDescriptor(
    db_name=cfg_dbs.user,
    **_config["src_tables"]["css"],
)
data_desc_srets = CDataDescriptor(
    codes=sectors,
    db_name=cfg_dbs.user,
    **_config["src_tables"]["srets"],
)

data_desc_pv1m = CDataDescriptor(codes=cfg.codes, **_config["src_tables"]["pv1m"])
data_desc_cpv = CDataDescriptor(codes=cfg.codes, **_config["src_tables"]["cpv"])

"""
-----------------
--- user data ---
-----------------
"""

data_desc_fac_raw = CDataDescriptor(
    db_name=cfg_dbs.user,
    table_name=cfg_tables.fac_raw,
    codes=cfg.codes,
    fields=cfg.factors.to_list(),
    lag=20,
    data_view_type="data3d",
)
data_desc_fac_nrm = CDataDescriptor(
    db_name=cfg_dbs.user,
    table_name=cfg_tables.fac_nrm,
    codes=cfg.codes,
    fields=cfg.factors.to_list(),
    lag=20,
    data_view_type="data3d",
)
data_desc_fac_sig = CDataDescriptor(
    db_name=cfg_dbs.user,
    table_name=cfg_tables.fac_sig,
    codes=cfg.codes,
    fields=cfg.factors.to_list(),
    lag=20,
    data_view_type="data3d",
)
data_desc_fac_ewa = CDataDescriptor(
    db_name=cfg_dbs.user,
    table_name=cfg_tables.fac_ewa,
    codes=cfg.codes,
    fields=cfg.factors.to_list(),
    lag=20,
    data_view_type="data3d",
)


mkt_desc_fut = CMarketDescriptor(
    matcher="daily",
    ini_cash=cfg.csim.init_cash,
    fee_rate=cfg.csim.cost_rate_pri,
    account="detail",
    data=(cfg_dbs.user, "preprocess"),
    settle_price_table=(cfg_dbs.user, "preprocess"),
    settle_price_field="close_major",
    open_field="open_major",
    close_field="close_major",
    multiplier_field="multiplier_major",
    limit_up_field="close_major",
    limit_down_field="close_major",
    dominant_contract_table=(cfg_dbs.user, "dominant"),
)

if __name__ == "__main__":
    from qtools_sxzq.qwidgets import check_and_mkdir

    check_and_mkdir(cfg.project_data_dir)

    sep = lambda z: f"\n{z:-^60s}"
    print(sep("universe"))
    i = 0
    for k, v in universe.items():
        print(f"{i:02d} | {k:<11s} | {v}")
        i += 1
    print(sep("cfg"))
    print(cfg)
    print(sep("cfg_tables"))
    print(cfg_tables)
    print(sep("cfg_dbs"))
    print(cfg_dbs)
    print(sep("data_desc_preprocess"))
    print(data_desc_preprocess)
    print(sep("data_desc_dominant"))
    print(data_desc_dominant)
    print(sep("data_desc_pv1m"))
    print(data_desc_pv1m)
    print(sep("data_desc_cpv"))
    print(data_desc_cpv)

    print(sep("factors"))
    cfg.factors.display()
    print(sep("risk factors"))
    [print(f) for f in cfg.factors.get_risk_factors()]
