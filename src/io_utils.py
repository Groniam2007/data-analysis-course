"""Чтение и запись форматов, отладочный срез, замеры времени."""

import logging
import random
import statistics
import time
from pathlib import Path

import pandas as pd
import polars as pl

from src import config

log = logging.getLogger(__name__)


def file_size_mb(path: Path) -> float:
    return Path(path).stat().st_size / 1024**2


def count_lines(path: Path, chunk: int = 1 << 24) -> int:
    """Число строк файла без его загрузки в память."""
    n = 0
    with open(path, "rb") as f:
        while block := f.read(chunk):
            n += block.count(b"\n")
    return n


def peek(path: Path, n: int = 100) -> list[str]:
    """Первые n строк файла - для определения структуры без полного чтения."""
    with open(path, encoding="utf-8") as f:
        return [next(f).rstrip("\n") for _ in range(n)]


def make_sample(
    src: Path = config.GEONAMES_CSV,
    dst: Path = config.SAMPLE_CSV,
    n: int = config.SAMPLE_ROWS,
    seed: int = config.SEED,
) -> Path:
    """Случайный срез из n записей потоковым чтением.

    Файл упорядочен по странам, поэтому первые n строк дали бы срез из трёх
    стран. Номера строк выбираются заранее, затем файл читается построчно, и в
    памяти никогда не находится больше одной строки исходника.
    """
    total = count_lines(src) - 1
    keep = set(random.Random(seed).sample(range(total), min(n, total)))
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(src, encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
        fout.write(next(fin))
        for i, line in enumerate(fin):
            if i in keep:
                fout.write(line)
    log.info("Срез: %d из %d записей -> %s", len(keep), total, dst)
    return dst


def read_geonames(
    path: Path = config.GEONAMES_CSV, typed: bool = True, **kwargs
) -> pd.DataFrame:
    """Чтение исходного CSV средствами pandas.

    typed=False - типы угадывает библиотека (состояние «до оптимизации»),
    typed=True - схема из config применяется сразу при чтении.
    """
    opts = dict(config.GEONAMES_READ_OPTIONS)
    if typed:
        opts["dtype"] = config.GEONAMES_SCHEMA
        opts["parse_dates"] = config.GEONAMES_DATE_COLUMNS
    opts.update(kwargs)
    return pd.read_csv(path, **opts)


def scan_geonames(path: Path = config.GEONAMES_CSV) -> pl.LazyFrame:
    """Ленивое чтение исходного CSV средствами polars, все столбцы как строки."""
    return pl.scan_csv(path, quote_char=None, infer_schema=False, encoding="utf8")


def read_health(path: Path = config.HEALTH_CSV) -> pl.DataFrame:
    return pl.read_csv(path, infer_schema_length=None, null_values=[""])


def read_wdi(path: Path = config.WDI_CSV) -> pl.DataFrame:
    # Строки заканчиваются лишней запятой - отсюда безымянный пустой столбец.
    return pl.read_csv(path, infer_schema=False, null_values=[""])


def timed(func, repeats: int = 3, warmup: bool = True) -> dict:
    """Медиана времени выполнения func по repeats повторам.

    Первый запуск отбрасывается: после него файл находится в кэше ОС.
    """
    if warmup:
        func()
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        func()
        times.append(time.perf_counter() - t0)
    return {"median": statistics.median(times), "runs": times}
