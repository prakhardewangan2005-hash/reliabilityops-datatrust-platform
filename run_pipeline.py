from __future__ import annotations

import argparse
from pathlib import Path

from src.config import settings
from src.db import get_engine, execute_sql_file
from src.ingest import ingest_dir
from src.transform import run_transforms
from src.dq import run_dq
from src.logging_utils import setup_logging

def init_db(engine):
    root = Path(__file__).resolve().parent
    execute_sql_file(engine, str(root / "sql" / "00_schema.sql"))

def main():
    setup_logging("INFO")
    parser = argparse.ArgumentParser(prog="reliabilityops-datatrust-platform")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db")
    sub.add_parser("ingest")
    sub.add_parser("transform")
    sub.add_parser("dq")
    sub.add_parser("run")

    args = parser.parse_args()
    engine = get_engine(settings.db_url)

    if args.cmd == "init-db":
        init_db(engine)
        print("✅ DB initialized")

    elif args.cmd == "ingest":
        ingest_dir(engine, settings.data_dir)
        print("✅ Ingest complete")

    elif args.cmd == "transform":
        run_transforms(engine)
        print("✅ Transform + mart complete")

    elif args.cmd == "dq":
        run_dq(engine, "dq/checks.yml", strict=settings.strict_dq)
        print("✅ DQ complete")

    elif args.cmd == "run":
        init_db(engine)
        ingest_dir(engine, settings.data_dir)
        run_transforms(engine)
        run_dq(engine, "dq/checks.yml", strict=settings.strict_dq)
        print("✅ Pipeline run complete")

if __name__ == "__main__":
    main()
