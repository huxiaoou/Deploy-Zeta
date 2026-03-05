import argparse


def parse_args():
    arg_parser = argparse.ArgumentParser(description="Entry point of this project")
    arg_parser.add_argument("--bgn", type=str, help="begin date, format = [YYYYMMDD]", required=True)
    arg_parser.add_argument("--end", type=str, help="stop  date, format = [YYYYMMDD]", required=True)
    arg_parser_subs = arg_parser.add_subparsers(
        title="Position argument to call sub functions",
        dest="switch",
        description="use this position argument to call different functions of this project. "
        "For example: 'python main.py --bgn 20120104 --end 20240826 factors --type raw'",
        required=True,
    )

    # switch: factors
    arg_parser_sub = arg_parser_subs.add_parser(name="factors", help="Calculate factors")
    arg_parser_sub.add_argument("--type", type=str, choices=("raw", "nrm", "sig", "ewa"))

    # switch: simulations
    arg_parser_sub = arg_parser_subs.add_parser(name="simulations", help="do simulations")
    arg_parser_sub.add_argument("--type", type=str, choices=("fac", "stg"))
    arg_parser_sub.add_argument("--omit", default=False, action="store_true")

    # switch: optimize
    arg_parser_sub = arg_parser_subs.add_parser(name="optimize", help="Optimize weights for factors or sectors")
    arg_parser_sub.add_argument("--type", type=str, choices=("fac",))

    # switch: signals
    arg_parser_sub = arg_parser_subs.add_parser(name="signals", help="generate signals")

    return arg_parser.parse_args()


if __name__ == "__main__":
    from config import (
        cfg,
        cfg_tables,
        cfg_dbs,
        data_desc_fac_raw,
        data_desc_fac_nrm,
        data_desc_fac_sig,
        data_desc_fac_ewa,
        data_desc_optimize_fac,
        data_desc_sim_fac,
        data_desc_sig_stg,
        data_desc_preprocess,
        data_desc_pv1m,
        data_desc_avlb,
        data_desc_css,
        data_desc_icov,
    )

    args = parse_args()
    span: tuple[str, str] = (args.bgn, args.end)
    codes: list[str] = cfg.codes

    if args.switch == "factors":
        if args.type == "raw":
            from solutions.factors import main_process_factors_raw

            data_desc_preprocess.lag = data_desc_pv1m.lag = cfg.factors.lag
            main_process_factors_raw(
                span=span,
                codes=codes,
                cfg_factors=cfg.factors,
                data_desc_pv=data_desc_preprocess,
                data_desc_pv1m=data_desc_pv1m,
                dst_db=cfg_dbs.user,
                table_fac_raw=cfg_tables.fac_raw,
            )
        elif args.type == "nrm":
            from solutions.nrm import main_process_factors_nrm
            from config import universe_sector

            main_process_factors_nrm(
                span=span,
                codes=codes,
                cfg_factors=cfg.factors,
                data_desc_avlb=data_desc_avlb,
                data_desc_fac_raw=data_desc_fac_raw,
                universe_sector=universe_sector,
                dst_db=cfg_dbs.user,
                table_fac_neu=cfg_tables.fac_nrm,
            )
        elif args.type == "sig":
            from solutions.sig import main_process_factor_sig

            main_process_factor_sig(
                span=span,
                codes=codes,
                cfg_factors=cfg.factors,
                data_desc_avlb=data_desc_avlb,
                data_desc_fac_nrm=data_desc_fac_nrm,
                dst_db=cfg_dbs.user,
                table_fac_sig=cfg_tables.fac_sig,
            )
        elif args.type == "ewa":
            from solutions.ewa import main_process_factor_ewa

            data_desc_fac_sig.lag = cfg.factors.lag
            main_process_factor_ewa(
                span=span,
                codes=codes,
                cfg_factors=cfg.factors,
                data_desc_fac_sig=data_desc_fac_sig,
                dst_db=cfg_dbs.user,
                table_fac_ewa=cfg_tables.fac_ewa,
            )
    elif args.switch == "simulations":
        if args.type == "fac":
            from solutions.qsim import CSimQuick

            sim_quick = CSimQuick(
                codes=codes,
                cfg_factors=cfg.factors,
                data_desc_pv=data_desc_preprocess,
                data_desc_fac_ewa=data_desc_fac_ewa,
                tgt_rets=cfg.tgt_rets,
                cost_rate=cfg.csim.cost_rate_sub,
                dst_db=cfg_dbs.user,
                table_sim_fac=cfg_tables.sim_fac,
                project_data_dir=cfg.project_data_dir,
                vid=cfg.vid,
            )
            sim_quick.main(span=span, ret_win=cfg.qsim.win)
        elif args.type == "stg":
            import os
            from solutions.csim import main_process_sim_cmplx
            from solutions.csim import main_process_sim_dual_sub
            from solutions.eval import CMultiEvaluator
            from config import mkt_desc_fut, universe

            # mkt_desc_fut.settle_price_field = "close"
            for tgt_ret in cfg.tgt_rets:
                exe_price = "open_major" if tgt_ret == "opn" else "close_major"
                main_process_sim_cmplx(
                    span=span,
                    codes=codes,
                    sig=tgt_ret,
                    data_desc_sig=data_desc_sig_stg,
                    exe_price=exe_price,
                    oi_cap_ratio=cfg.csim.oi_cap_ratio,
                    data_desc_pv=data_desc_preprocess,
                    mkt_desc_fut=mkt_desc_fut,
                    project_data_dir=cfg.project_data_dir,
                    universe=universe,
                    vid=cfg.vid,
                    using_sxzq_dlz=not args.omit,
                )
            sig_0, sig_1 = "opn", "cls"
            exe_price_0, exe_price_1 = "open_major", "close_major"
            main_process_sim_dual_sub(
                span=span,
                codes=codes,
                sig_0=sig_0,
                exe_price_0=exe_price_0,
                sig_1=sig_1,
                exe_price_1=exe_price_1,
                oi_cap_ratio=cfg.csim.oi_cap_ratio,
                data_desc_sig=data_desc_sig_stg,
                data_desc_pv=data_desc_preprocess,
                mkt_desc_fut=mkt_desc_fut,
                project_data_dir=cfg.project_data_dir,
                universe=universe,
                vid=cfg.vid,
                using_sxzq_dlz=not args.omit,
            )
            mulit_evaluator = CMultiEvaluator(
                perf_paths=[
                    os.path.join(cfg.project_data_dir, "perfs", f"perf_{sig_0}-{exe_price_0}.{cfg.vid}.csv"),
                    os.path.join(cfg.project_data_dir, "perfs", f"perf_{sig_1}-{exe_price_1}.{cfg.vid}.csv"),
                    os.path.join(cfg.project_data_dir, "perfs", f"perf_dualSubs.{cfg.vid}.csv"),
                ],
                ret_lbl="日收益率",
                date_lbl="date",
                short_ids=["open", "close", "dual"],
                by_year_ids=["dual"],
                project_data_dir=cfg.project_data_dir,
                src_id="csim",
                vid=cfg.vid,
            )
            mulit_evaluator.main()
    elif args.switch == "optimize":
        if args.type == "fac":
            from solutions.optimize import main_process_optimize_fac_wgt

            main_process_optimize_fac_wgt(
                span=span,
                factors=cfg.factors.to_list(),
                tgt_rets=cfg.tgt_rets,
                cfg_optimizer_fac=cfg.optimizer_fac,
                data_desc_sim=data_desc_sim_fac,
                dst_db=cfg_dbs.user,
                table_optimize_fac=cfg_tables.optimize_fac,
            )
    elif args.switch == "signals":
        from solutions.sig import main_process_signals_stg

        main_process_signals_stg(
            span=span,
            codes=codes,
            tgt_rets=cfg.tgt_rets,
            factors=cfg.factors.to_list(),
            data_desc_sig_fac=data_desc_fac_ewa,
            data_desc_optimize_fac=data_desc_optimize_fac,
            data_desc_css=data_desc_css,
            data_desc_icov=data_desc_icov,
            data_desc_avlb=data_desc_avlb,
            dst_db=cfg_dbs.user,
            table_sig_stg=cfg_tables.sig_stg,
        )
