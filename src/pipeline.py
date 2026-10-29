"""Сквозной сценарий обработки: raw -> interim -> processed.

    python -m src.pipeline                       # полный набор, все этапы
    python -m src.pipeline --sample              # то же на отладочном срезе
    python -m src.pipeline --stage clean --sample
    python -m src.pipeline --input data/raw/geonames.csv --output data/processed/clean.parquet
"""

import argparse
import logging
import sys
import time
import warnings
from pathlib import Path

import pandas as pd
import polars as pl

from src import cleaning, config, io_utils

log = logging.getLogger("pipeline")

STAGES = ["sample", "serialize", "typed", "clean"]


def setup_logging(name: str = "pipeline") -> None:
    config.LOGS_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(config.LOGS_DIR / f"{name}.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


def read_raw(path: Path, typed: bool) -> pd.DataFrame:
    """Чтение исходника; повреждённые строки пропускаются и попадают в журнал."""
    if not path.exists():
        raise FileNotFoundError(f"исходный файл не найден: {path}")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", pd.errors.ParserWarning)
        # low_memory=False: без схемы pandas угадывает тип по кускам файла, и в
        # одном столбце admin-кодов оказываются и числа (9 вместо 09), и строки.
        df = io_utils.read_geonames(
            path, typed=typed, on_bad_lines="warn", low_memory=typed
        )
    for w in caught:
        if issubclass(w.category, pd.errors.ParserWarning):
            log.warning("повреждённая строка пропущена: %s", str(w.message).strip())
    expected = list(config.GEONAMES_SCHEMA) + config.GEONAMES_DATE_COLUMNS
    if list(df.columns) != expected:
        raise ValueError(f"неверный формат: столбцы {list(df.columns)}")
    return df


def stage_sample(args) -> None:
    io_utils.make_sample(config.GEONAMES_CSV, config.SAMPLE_CSV)


def stage_serialize(args) -> None:
    """Исходный CSV один раз разбирается и сохраняется в Parquet без схемы."""
    df = read_raw(args.input, typed=False)
    out = config.variant(config.SOURCE_PARQUET, args.sample)
    df.to_parquet(out, compression="snappy", index=False)
    log.info("serialize: %d строк -> %s (%.1f МБ)", len(df), out, io_utils.file_size_mb(out))


def stage_typed(args) -> None:
    """Схема типов применяется сразу при чтении исходника."""
    df = read_raw(args.input, typed=True)
    out = config.variant(config.TYPED_PARQUET, args.sample)
    df.to_parquet(out, compression="zstd", index=False)
    log.info("typed: %d строк -> %s (%.1f МБ)", len(df), out, io_utils.file_size_mb(out))


def run_cleaning(geonames: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Все этапы очистки; возвращает таблицы для каталога processed."""
    cleaning.reset_stats()
    for step in cleaning.GEONAMES_STEPS:
        geonames = step(geonames)
    health = cleaning.clean_health(io_utils.read_health())
    countries = cleaning.build_countries(geonames, health)
    geonames = cleaning.join_countries(geonames, countries)
    indicators, wdi = cleaning.clean_wdi(io_utils.read_wdi(), countries)
    return {
        "clean": cleaning.finalize(geonames),
        "alternate_names": cleaning.explode_alternate_names(geonames),
        "geoname_cc2": cleaning.explode_cc2(geonames),
        "countries": countries,
        "health_indicators": health,
        "indicators": indicators,
        "wdi_values": wdi,
    }


OUTPUTS = {
    "alternate_names": config.ALT_NAMES_PARQUET,
    "geoname_cc2": config.CC2_PARQUET,
    "countries": config.COUNTRIES_PARQUET,
    "health_indicators": config.HEALTH_PARQUET,
    "indicators": config.INDICATORS_PARQUET,
    "wdi_values": config.WDI_PARQUET,
}


def stage_clean(args) -> None:
    src = config.variant(config.TYPED_PARQUET, args.sample)
    if not src.exists():
        raise FileNotFoundError(f"нет типизированного набора {src}: запустите --stage typed")
    tables = run_cleaning(pl.read_parquet(src))
    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    tables["clean"].write_parquet(args.output, compression="zstd")
    for name, path in OUTPUTS.items():
        tables[name].write_parquet(config.variant(path, args.sample), compression="zstd")
    stats = cleaning.stats_frame()
    stats.write_csv(config.LOGS_DIR / f"cleaning_stats{'_sample' if args.sample else ''}.csv")
    first, last = stats["rows_in"][0], tables["clean"].height
    log.info("clean: было %d, стало %d, потеряно %.3f%%", first, last,
             100 * (first - last) / first)  # fmt: skip


HANDLERS = {
    "sample": stage_sample,
    "serialize": stage_serialize,
    "typed": stage_typed,
    "clean": stage_clean,
}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Подготовка и очистка набора GeoNames")
    p.add_argument("--input", type=Path, help="исходный CSV (по умолчанию data/raw/geonames.csv)")
    p.add_argument("--output", type=Path, help="очищенный Parquet")
    p.add_argument("--sample", action="store_true", help="работать на отладочном срезе")
    p.add_argument("--stage", choices=["all", *STAGES], default="all")
    args = p.parse_args(argv)
    if args.input is None:
        args.input = config.SAMPLE_CSV if args.sample else config.GEONAMES_CSV
    if args.output is None:
        args.output = config.variant(config.CLEAN_PARQUET, args.sample)
    return args


def main(argv=None) -> int:
    setup_logging()
    args = parse_args(argv)
    if args.stage == "all":
        stages = STAGES if args.sample else STAGES[1:]
    else:
        stages = [args.stage]
    config.INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    try:
        for name in stages:
            t0 = time.perf_counter()
            log.info("этап %s: начало (%s)", name, "срез" if args.sample else "полный набор")
            HANDLERS[name](args)
            log.info("этап %s: %.1f c", name, time.perf_counter() - t0)
    except FileNotFoundError as e:
        log.error("%s", e)
        return 2
    except (ValueError, pd.errors.ParserError, pl.exceptions.ComputeError) as e:
        log.error("неверный формат данных: %s", e)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
