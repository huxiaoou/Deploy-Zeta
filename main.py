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

    return arg_parser.parse_args()


if __name__ == "__main__":
    from config import (
        cfg,
        cfg_tables,
        cfg_dbs,
        data_desc_preprocess,
        data_desc_pv1m,
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
