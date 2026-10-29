"""Загрузка очищенных данных в SQLite и PostgreSQL по схеме в 3НФ.

    python -m src.db --target sqlite [--sample]
    python -m src.db --target postgres [--sample]
"""

import argparse
import io
import json
import logging
import os
import sqlite3
import statistics
import sys
import time

import polars as pl
import psycopg2
import psycopg2.extras
import pycountry
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from src import config

log = logging.getLogger("db")

BATCH = 50_000

# Порядок загрузки: сначала справочники, затем таблицы со ссылками на них.
LOAD_ORDER = [
    "regions", "income_levels", "countries", "feature_classes", "feature_codes",
    "timezones", "geonames", "alternate_names", "geoname_cc2",
    "health_indicators", "indicators", "wdi_values",
]  # fmt: skip

INDEXES = [
    "CREATE INDEX idx_geonames_country ON geonames(country_code)",
    "CREATE INDEX idx_geonames_feature ON geonames(feature_code_id)",
    "CREATE INDEX idx_geonames_date ON geonames(modification_date)",
    "CREATE INDEX idx_alt_names_name ON alternate_names(alt_name)",
]

QUERIES = {
    "count": "SELECT COUNT(*) FROM geonames",
    "aggregate": """
        SELECT fc.feature_class, COUNT(*) AS objects,
               AVG(g.elevation) AS avg_elevation, SUM(g.population) AS population
        FROM geonames g JOIN feature_codes fc ON fc.feature_code_id = g.feature_code_id
        GROUP BY fc.feature_class ORDER BY objects DESC""",
    "date_range": """
        SELECT geonameid, name, population FROM geonames
        WHERE modification_date BETWEEN '2015-06-01' AND '2015-06-07'""",
    "region_join": """
        SELECT r.region_name, COUNT(*) AS objects, MAX(g.population) AS max_population
        FROM geonames g
        JOIN countries c ON c.country_code = g.country_code
        JOIN regions r ON r.region_id = c.region_id
        GROUP BY r.region_name ORDER BY objects DESC""",
}


# ----------------------------------------------------------- подключения


def sqlite_path() -> str:
    load_dotenv(config.ROOT / ".env")
    return str(config.ROOT / os.getenv("SQLITE_PATH", "data/processed/dataset.db"))


def pg_url() -> str:
    load_dotenv(config.ROOT / ".env")
    return (
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )


def pg_connect():
    load_dotenv(config.ROOT / ".env")
    return psycopg2.connect(
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"), dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"),
    )  # fmt: skip


# ------------------------------------------------ нормализованные таблицы


def _iso3_to_iso2(iso3: str) -> str | None:
    c = pycountry.countries.get(alpha_3=iso3)
    if c is not None:
        return c.alpha_2
    return {v[0]: k for k, v in config.LEGACY_COUNTRIES.items()}.get(iso3)


def build_tables(sample: bool = False) -> dict[str, pl.DataFrame]:
    """Разбиение очищенного набора на справочники и основные таблицы (3НФ)."""
    p = lambda path: config.variant(path, sample)  # noqa: E731
    geo = pl.read_parquet(p(config.CLEAN_PARQUET)).with_columns(
        pl.col(pl.Categorical).cast(pl.String)
    )
    countries = pl.read_parquet(p(config.COUNTRIES_PARQUET))
    health = pl.read_parquet(p(config.HEALTH_PARQUET))

    regions = _dictionary(countries["region"], "region_id", "region_name")
    incomes = _dictionary(countries["income_level"], "income_level_id", "income_level_name")
    countries_t = (
        countries.join(regions, left_on="region", right_on="region_name", how="left")
        .join(incomes, left_on="income_level", right_on="income_level_name", how="left")
        .select(pl.col("iso2").alias("country_code"), "iso3", "country_name",
                "region_id", "income_level_id")  # fmt: skip
    )
    classes = pl.DataFrame(
        {"feature_class": list(config.FEATURE_CLASSES),
         "description": list(config.FEATURE_CLASSES.values())}  # fmt: skip
    )
    # Пара (класс, код); у записей без кода класс сохраняется строкой с кодом NULL.
    codes = (
        geo.select("feature_class", "feature_code").unique()
        .filter(pl.col("feature_class").is_not_null())
        .sort(["feature_class", "feature_code"], nulls_last=True)
        .with_row_index("feature_code_id", offset=1)
        .with_columns(pl.col("feature_code_id").cast(pl.Int16))
        .select("feature_code_id", "feature_code", "feature_class")
    )  # fmt: skip
    timezones = _dictionary(geo["timezone"], "timezone_id", "timezone_name")

    geonames = (
        geo.join(codes, on=["feature_class", "feature_code"], how="left", nulls_equal=True)
        .join(timezones, left_on="timezone", right_on="timezone_name", how="left")
        .select(
            "geonameid", "name", "asciiname", "latitude", "longitude",
            "feature_code_id", "country_code", "admin1_code", "admin2_code",
            "admin3_code", "admin4_code", "population", "elevation", "dem",
            "timezone_id", "modification_date", "population_outlier",
            "elevation_outlier", "elevation_unit_suspect",
        )
    )  # fmt: skip
    assert geonames.height == geo.height, "соединение со справочниками размножило строки"

    health_t = health.with_columns(
        pl.col("country_code").map_elements(_iso3_to_iso2, return_dtype=pl.String),
        pl.col("population").round().cast(pl.Int64),
    ).drop("country_name", "region", "income_level")
    wdi = pl.read_parquet(p(config.WDI_PARQUET)).with_columns(
        pl.col("country_code").map_elements(_iso3_to_iso2, return_dtype=pl.String)
    )
    return {
        "regions": regions,
        "income_levels": incomes,
        "countries": countries_t,
        "feature_classes": classes,
        "feature_codes": codes,
        "timezones": timezones,
        "geonames": geonames,
        "alternate_names": pl.read_parquet(p(config.ALT_NAMES_PARQUET)),
        "geoname_cc2": pl.read_parquet(p(config.CC2_PARQUET)),
        "health_indicators": health_t,
        "indicators": pl.read_parquet(p(config.INDICATORS_PARQUET)),
        "wdi_values": wdi,
    }


def _dictionary(values: pl.Series, id_col: str, name_col: str) -> pl.DataFrame:
    return (
        values.drop_nulls().unique().sort().to_frame(name_col)
        .with_row_index(id_col, offset=1)
        .with_columns(pl.col(id_col).cast(pl.Int16))
    )  # fmt: skip


def run_script(execute, path) -> None:
    sql = path.read_text(encoding="utf-8")
    for stmt in [s.strip() for s in sql.split(";")]:
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--"))
        if body.strip():
            execute(stmt)


# ------------------------------------------------------------ SQLite


def load_sqlite(tables: dict[str, pl.DataFrame], path: str) -> dict:
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    con.execute("PRAGMA foreign_keys = ON")
    run_script(con.execute, config.SQL_DIR / "schema_sqlite.sql")
    timings = {}
    t_all = time.perf_counter()
    for name in LOAD_ORDER:
        df = tables[name]
        t0 = time.perf_counter()
        cols = ", ".join(df.columns)
        marks = ", ".join("?" * len(df.columns))
        sql = f"INSERT INTO {name} ({cols}) VALUES ({marks})"
        for batch in df.iter_slices(BATCH):
            rows = batch.with_columns(pl.col(pl.Date).cast(pl.String)).rows()
            con.executemany(sql, rows)
            con.commit()
        timings[name] = time.perf_counter() - t0
        log.info("sqlite %s: %d строк за %.1f c", name, df.height, timings[name])
    total = time.perf_counter() - t_all
    con.close()
    return {"load_seconds": total, "tables": timings}


# --------------------------------------------------------- PostgreSQL


def pg_insert_batches(cur, name: str, df: pl.DataFrame, page: int = 10_000) -> None:
    """Пакетная вставка: многострочный INSERT ... VALUES по page строк."""
    sql = f"INSERT INTO {name} ({', '.join(df.columns)}) VALUES %s"
    for batch in df.iter_slices(BATCH):
        psycopg2.extras.execute_values(cur, sql, batch.rows(), page_size=page)


def pg_copy(cur, name: str, df: pl.DataFrame, chunk: int = 1_000_000) -> None:
    """Потоковая загрузка командой COPY из CSV в памяти."""
    sql = f"COPY {name} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')"
    for batch in df.iter_slices(chunk):
        buf = io.BytesIO()
        batch.write_csv(buf, include_header=False, null_value="")
        buf.seek(0)
        cur.copy_expert(sql, buf)


def load_postgres(tables: dict[str, pl.DataFrame], compare_on: str = "geonames") -> dict:
    con = pg_connect()
    cur = con.cursor()
    cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    run_script(cur.execute, config.SQL_DIR / "schema_postgres.sql")
    con.commit()
    timings = {}

    # Сравнение способов загрузки на основной таблице.
    for name in LOAD_ORDER[: LOAD_ORDER.index(compare_on)]:
        pg_copy(cur, name, tables[name])
    con.commit()
    t0 = time.perf_counter()
    pg_insert_batches(cur, compare_on, tables[compare_on])
    con.commit()
    batch_s = time.perf_counter() - t0
    log.info("postgres %s пакетами: %.1f c", compare_on, batch_s)
    cur.execute(f"TRUNCATE {compare_on} CASCADE")
    con.commit()
    t0 = time.perf_counter()
    pg_copy(cur, compare_on, tables[compare_on])
    con.commit()
    copy_s = time.perf_counter() - t0
    log.info("postgres %s через COPY: %.1f c", compare_on, copy_s)

    # Полная загрузка всех таблиц через COPY - её время идёт в сравнение СУБД.
    cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    run_script(cur.execute, config.SQL_DIR / "schema_postgres.sql")
    con.commit()
    t_all = time.perf_counter()
    for name in LOAD_ORDER:
        t0 = time.perf_counter()
        pg_copy(cur, name, tables[name])
        con.commit()
        timings[name] = time.perf_counter() - t0
        log.info("postgres %s: %d строк за %.1f c", name, tables[name].height, timings[name])
    total = time.perf_counter() - t_all
    con.autocommit = True
    cur.execute("VACUUM ANALYZE")
    cur.close()
    con.close()
    return {"load_seconds": total, "tables": timings,
            "geonames_batch_insert_seconds": batch_s,
            "geonames_copy_seconds": copy_s}  # fmt: skip


# ------------------------------------------------ индексы и запросы


def time_query(execute_fetch, sql: str, repeats: int = 3) -> dict:
    execute_fetch(sql)  # прогрев кэша
    runs = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        rows = execute_fetch(sql)
        runs.append(time.perf_counter() - t0)
    return {"median": statistics.median(runs), "runs": runs, "rows": len(rows)}


def benchmark_queries(engine_url: str, analyze: bool = False) -> dict:
    eng = create_engine(engine_url)
    result = {}
    with eng.connect() as con:
        fetch = lambda sql: con.execute(text(sql)).fetchall()  # noqa: E731
        for name, sql in QUERIES.items():
            result[name] = time_query(fetch, sql)
        result["samples"] = {
            name: [list(map(_jsonable, r)) for r in fetch(sql)[:10]]
            for name, sql in QUERIES.items()
        }
        plan_sql = ("EXPLAIN ANALYZE " if analyze else "EXPLAIN QUERY PLAN ") + QUERIES["date_range"]
        result["date_range_plan"] = [str(r) for r in fetch(plan_sql)]
    eng.dispose()
    return result


def _jsonable(v):
    return v if isinstance(v, (int, float, str, type(None))) else str(v)


def create_indexes(engine_url: str) -> float:
    eng = create_engine(engine_url)
    t0 = time.perf_counter()
    with eng.begin() as con:
        for stmt in INDEXES:
            con.execute(text(stmt))
        con.execute(text("ANALYZE"))
    eng.dispose()
    return time.perf_counter() - t0


def storage_mb(target: str) -> float:
    if target == "sqlite":
        return os.path.getsize(sqlite_path()) / 1024**2
    eng = create_engine(pg_url())
    with eng.connect() as con:
        size = con.execute(text("SELECT pg_database_size(current_database())")).scalar()
    eng.dispose()
    return size / 1024**2


def main(argv=None) -> int:
    from src.pipeline import setup_logging

    setup_logging("db")
    p = argparse.ArgumentParser(description="Загрузка очищенных данных в СУБД")
    p.add_argument("--target", choices=["sqlite", "postgres"], required=True)
    p.add_argument("--sample", action="store_true")
    args = p.parse_args(argv)

    tables = build_tables(args.sample)
    for name in LOAD_ORDER:
        log.info("таблица %s: %d строк", name, tables[name].height)
    if args.target == "sqlite":
        load = load_sqlite(tables, sqlite_path())
        url = f"sqlite:///{sqlite_path()}"
    else:
        load = load_postgres(tables)
        url = pg_url()
    before = benchmark_queries(url, analyze=args.target == "postgres")
    index_s = create_indexes(url)
    after = benchmark_queries(url, analyze=args.target == "postgres")
    result = {
        "target": args.target, "sample": args.sample, **load,
        "rows": {n: tables[n].height for n in LOAD_ORDER},
        "storage_mb": storage_mb(args.target),
        "index_seconds": index_s, "queries_before_index": before,
        "queries_after_index": after,
    }  # fmt: skip
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.RESULTS_DIR / f"db_{args.target}{'_sample' if args.sample else ''}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("результаты: %s", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
