from pathlib import Path

import duckdb

# datacontract-cli has no support for servers.type: duckdb as a live
# database connection — it only executes quality checks against local
# file servers (csv/json/parquet/delta) or named DB connectors (postgres,
# snowflake, etc). Gold tables are exported to Parquet here so the
# contracts' servers.local block can point at real files datacontract
# test can actually read.
DB_PATH = Path(__file__).resolve().parent.parent / "f1.duckdb"
EXPORT_DIR = Path(__file__).resolve().parent.parent / "exports"

# contract model name -> gold table name (dbt model file names are
# prefixed with gold_, the contracts use the bare model name)
GOLD_MODELS = {
    "sessions": "gold_sessions",
    "lap_times": "gold_lap_times",
    "driver_season_stats": "gold_driver_season_stats",
}


def export_gold_to_parquet():
    EXPORT_DIR.mkdir(exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    for contract_model, table_name in GOLD_MODELS.items():
        target = EXPORT_DIR / f"{contract_model}.parquet"
        con.execute(f"copy gold.{table_name} to '{target}' (format parquet)")
        print(f"Exported gold.{table_name} -> {target}")
    con.close()


if __name__ == "__main__":
    export_gold_to_parquet()
