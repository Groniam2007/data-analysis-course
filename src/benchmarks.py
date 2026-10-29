"""Замеры шагов 9-11: форматы хранения, схема типов, pandas против polars.

    python -m src.benchmarks formats
    python -m src.benchmarks schema
    python -m src.benchmarks libs
    python -m src.benchmarks plots

Каждый замер - медиана трёх повторов после прогревочного запуска. Пиковая
память измеряется в отдельном процессе (peak working set по psutil), чтобы
одна библиотека не влияла на показания другой.
"""

import json
import platform
import subprocess
import sys
import time

import pandas as pd
import polars as pl
import psutil
import pyarrow

from src import config, io_utils
from src.io_utils import file_size_mb, timed

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]  # фиксированный порядок
INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _save(name: str, data: dict) -> None:
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = config.RESULTS_DIR / f"{name}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"-> {path}")


def _load(name: str) -> dict:
    return json.loads((config.RESULTS_DIR / f"{name}.json").read_text(encoding="utf-8"))


def environment() -> dict:
    return {
        "python": platform.python_version(),
        "pandas": pd.__version__,
        "polars": pl.__version__,
        "pyarrow": pyarrow.__version__,
        "os": platform.platform(),
        "cpu": platform.processor(),
        "cores": psutil.cpu_count(logical=False),
        "threads": psutil.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / 1024**3, 1),
        "rows": 11_540_325,
        "raw_csv_mb": round(file_size_mb(config.GEONAMES_CSV), 1),
    }


def peak_mb() -> float:
    info = psutil.Process().memory_info()
    return getattr(info, "peak_wset", info.rss) / 1024**2


# --------------------------------------------------------- шаг 9: форматы


def bench_formats() -> dict:
    tmp_csv = config.INTERIM_DIR / "geonames_copy.csv"
    read_csv = lambda: io_utils.read_geonames(typed=False, low_memory=False)  # noqa: E731
    df = read_csv()
    res = {"rows": len(df)}

    res["csv"] = {
        "size_mb": file_size_mb(config.GEONAMES_CSV),
        "read": timed(read_csv),
        "write": timed(lambda: df.to_csv(tmp_csv, index=False), warmup=False),
    }
    tmp_csv.unlink()
    formats = {
        "pickle": (config.SOURCE_PICKLE, lambda p: df.to_pickle(p), pd.read_pickle),
        "feather": (config.SOURCE_FEATHER, lambda p: df.to_feather(p), pd.read_feather),
        "parquet": (
            config.SOURCE_PARQUET,
            lambda p: df.to_parquet(p, compression="snappy", index=False),
            pd.read_parquet,
        ),
    }
    for name, (path, write, read) in formats.items():
        res[name] = {"write": timed(lambda: write(path), warmup=False)}
        res[name]["size_mb"] = file_size_mb(path)
        res[name]["read"] = timed(lambda: read(path))
        print(name, res[name]["size_mb"], res[name]["read"]["median"])
    cols = ["name", "country code", "population"]
    res["parquet_3cols"] = {
        "columns": cols,
        "read": timed(lambda: pd.read_parquet(config.SOURCE_PARQUET, columns=cols)),
    }
    res["environment"] = environment()
    _save("formats", res)
    return res


# ----------------------------------------------------- шаг 10: схема типов


def _memory_by_column(df: pd.DataFrame) -> dict:
    mem = df.memory_usage(deep=True, index=False) / 1024**2
    return {c: {"dtype": str(df[c].dtype), "mb": round(float(mem[c]), 2)} for c in df.columns}


def bench_schema() -> dict:
    before_path, after_path = config.SOURCE_PARQUET, config.TYPED_PARQUET
    before = pd.read_parquet(before_path)
    text_cols = [c for c in before.columns if not pd.api.types.is_numeric_dtype(before[c])]
    unique_share = {c: round(before[c].nunique() / len(before), 4) for c in text_cols}
    res = {"unique_share": unique_share}

    def group(df):
        return lambda: df.groupby("country code", observed=True)["dem"].mean()

    for label, path, df in [("before", before_path, before)]:
        res[label] = {
            "file_mb": file_size_mb(path),
            "memory_mb": df.memory_usage(deep=True).sum() / 1024**2,
            "columns": _memory_by_column(df),
            "read": timed(lambda: pd.read_parquet(path)),
            "groupby": timed(group(df)),
            "category_columns": int((df.dtypes == "category").sum()),
        }
    del before
    after = pd.read_parquet(after_path)
    res["after"] = {
        "file_mb": file_size_mb(after_path),
        "memory_mb": after.memory_usage(deep=True).sum() / 1024**2,
        "columns": _memory_by_column(after),
        "read": timed(lambda: pd.read_parquet(after_path)),
        "groupby": timed(group(after)),
        "category_columns": int((after.dtypes == "category").sum()),
    }
    # Тот же типизированный набор со сжатием snappy - чтобы отделить эффект
    # схемы от эффекта алгоритма сжатия.
    snappy = config.INTERIM_DIR / "geonames_typed_snappy.parquet"
    after.to_parquet(snappy, compression="snappy", index=False)
    res["after"]["file_mb_snappy"] = file_size_mb(snappy)
    snappy.unlink()
    _save("schema", res)
    return res


# ---------------------------------------------- шаг 11: pandas против polars


def _countries_pd() -> pd.DataFrame:
    return pd.read_parquet(config.COUNTRIES_PARQUET, columns=["iso2", "region"])


def worker(mode: str) -> dict:
    """Выполняется в отдельном процессе; печатает JSON с временем и памятью."""
    path = config.TYPED_PARQUET
    out = {}
    if mode == "pandas":
        out["read"] = timed(lambda: pd.read_parquet(path))
        df = pd.read_parquet(path)
        flt = lambda: df[(df["population"] > 0) & (df["feature class"] == "P")]  # noqa: E731
        out["filter"] = timed(flt)
        g = lambda: df.groupby("country code", observed=True)["population"].median()  # noqa: E731
        out["groupby"] = timed(g)
        agg = g()
        out["sort"] = timed(lambda: df.sort_values(["population", "geonameid"], ascending=False))
        dim = _countries_pd()
        out["join"] = timed(
            lambda: df.merge(dim, left_on="country code", right_on="iso2", how="left")
        )
        out["n_groups"] = len(agg)
    elif mode == "polars":
        out["read"] = timed(lambda: pl.read_parquet(path))
        df = pl.read_parquet(path)
        cond = (pl.col("population") > 0) & (pl.col("feature class") == "P")
        out["filter"] = timed(lambda: df.filter(cond))
        g = lambda: df.group_by("country code").agg(pl.col("population").median())  # noqa: E731
        out["groupby"] = timed(g)
        out["groupby_lazy"] = timed(
            lambda: pl.scan_parquet(path).group_by("country code")
            .agg(pl.col("population").median()).collect()
        )  # fmt: skip
        out["sort"] = timed(
            lambda: df.sort(["population", "geonameid"], descending=True)
        )
        dim = pl.read_parquet(config.COUNTRIES_PARQUET, columns=["iso2", "region"])
        df_s = df.with_columns(pl.col("country code").cast(pl.String))
        out["join"] = timed(
            lambda: df_s.join(dim, left_on="country code", right_on="iso2", how="left")
        )
        out["n_groups"] = g().height
    elif mode == "pandas_chain":
        out["chain"] = timed(
            lambda: pd.read_parquet(path)
            .query("population > 0 and `feature class` == 'P'")
            .groupby("country code", observed=True)["population"].median()
            .sort_values(ascending=False)
        )  # fmt: skip
    elif mode == "polars_lazy":
        out["chain"] = timed(
            lambda: pl.scan_parquet(path)
            .filter((pl.col("population") > 0) & (pl.col("feature class") == "P"))
            .group_by("country code").agg(pl.col("population").median())
            .sort("population", descending=True)
            .collect()
        )  # fmt: skip
    out["peak_mb"] = peak_mb()
    return out


def bench_libs() -> dict:
    res = {"environment": environment()}
    for mode in ["pandas", "polars", "pandas_chain", "polars_lazy"]:
        t0 = time.perf_counter()
        proc = subprocess.run(
            [sys.executable, "-X", "utf8", "-m", "src.benchmarks", "worker", mode],
            capture_output=True, text=True, encoding="utf-8", cwd=config.ROOT, check=True,
        )  # fmt: skip
        res[mode] = json.loads(proc.stdout.strip().splitlines()[-1])
        print(mode, f"{time.perf_counter() - t0:.0f} c", res[mode]["peak_mb"])
    _save("libs", res)
    return res


# ------------------------------------------------------------ диаграммы


def _style(ax, title: str, ylabel: str) -> None:
    ax.set_title(title, loc="left", color=INK, fontsize=12, pad=12)
    ax.set_ylabel(ylabel, color=INK_2)
    ax.set_facecolor(SURFACE)
    for side in ["top", "right", "left"]:
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK_2, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _label_bars(ax, bars, fmt: str = "{:.1f}") -> None:
    for b in bars:
        ax.annotate(fmt.format(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    ha="center", va="bottom", fontsize=9, color=INK,
                    xytext=(0, 3), textcoords="offset points")  # fmt: skip


def plots() -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    config.IMG_DIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "figure.facecolor": SURFACE})

    f = _load("formats")
    names = ["CSV", "Pickle", "Feather", "Parquet"]
    keys = ["csv", "pickle", "feather", "parquet"]
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.2))
    for ax, (metric, title, unit) in zip(axes, [
        ("size_mb", "Объём файла", "МБ"),
        ("write", "Время записи", "с"),
        ("read", "Время чтения", "с"),
    ]):  # fmt: skip
        vals = [f[k][metric] if metric == "size_mb" else f[k][metric]["median"] for k in keys]
        bars = ax.bar(names, vals, color=SERIES[0], width=0.6, edgecolor=SURFACE, linewidth=2)
        _style(ax, title, unit)
        _label_bars(ax, bars, "{:.0f}" if metric == "size_mb" else "{:.1f}")
    fig.suptitle("Форматы хранения: 11,5 млн строк GeoNames (медиана 3 повторов)",
                 x=0.01, ha="left", color=INK)  # fmt: skip
    fig.tight_layout()
    fig.savefig(config.IMG_DIR / "formats.png", dpi=150)
    plt.close(fig)

    s = _load("schema")
    fig, axes = plt.subplots(1, 4, figsize=(14, 4.2))
    for ax, (key, title, unit) in zip(axes, [
        ("file_mb", "Файл на диске", "МБ"), ("memory_mb", "Память", "МБ"),
        ("read", "Чтение", "с"), ("groupby", "Группировка", "с"),
    ]):  # fmt: skip
        vals = [s[w][key]["median"] if isinstance(s[w][key], dict) else s[w][key]
                for w in ["before", "after"]]  # fmt: skip
        bars = ax.bar(["до", "после"], vals, color=[SERIES[1], SERIES[0]], width=0.55,
                      edgecolor=SURFACE, linewidth=2)  # fmt: skip
        _style(ax, title, unit)
        _label_bars(ax, bars, "{:.0f}" if unit == "МБ" else "{:.2f}")
    fig.suptitle("Оптимизация схемы типов: до и после", x=0.01, ha="left", color=INK)
    fig.tight_layout()
    fig.savefig(config.IMG_DIR / "schema.png", dpi=150)
    plt.close(fig)

    lb = _load("libs")
    ops = [("read", "Чтение"), ("filter", "Фильтрация"), ("groupby", "Группировка"),
           ("sort", "Сортировка"), ("join", "Соединение")]  # fmt: skip
    pd_v = [lb["pandas"][k]["median"] for k, _ in ops] + [lb["pandas_chain"]["chain"]["median"]]
    pl_v = [lb["polars"][k]["median"] for k, _ in ops] + [lb["polars_lazy"]["chain"]["median"]]
    labels = [n for _, n in ops] + ["Цепочка\n(ленивая)"]
    x = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(11, 4.5))
    b1 = ax.bar(x - 0.2, pd_v, 0.38, label="pandas", color=SERIES[0], edgecolor=SURFACE, linewidth=2)
    b2 = ax.bar(x + 0.2, pl_v, 0.38, label="polars", color=SERIES[1], edgecolor=SURFACE, linewidth=2)
    _style(ax, "pandas и polars на одинаковых операциях (медиана, с)", "с")
    ax.set_xticks(x, labels)
    _label_bars(ax, b1, "{:.2f}")
    _label_bars(ax, b2, "{:.2f}")
    ax.legend(frameon=False, labelcolor=INK)
    fig.tight_layout()
    fig.savefig(config.IMG_DIR / "libs.png", dpi=150)
    plt.close(fig)

    try:
        sq, pg = _load("db_sqlite"), _load("db_postgres")
    except FileNotFoundError:
        return
    metrics = [
        ("Загрузка, с", sq["load_seconds"], pg["load_seconds"]),
        ("Агрегирование, с", sq["queries_after_index"]["aggregate"]["median"],
         pg["queries_after_index"]["aggregate"]["median"]),
        ("Диапазон дат\nбез индекса, с", sq["queries_before_index"]["date_range"]["median"],
         pg["queries_before_index"]["date_range"]["median"]),
        ("Диапазон дат\nс индексом, с", sq["queries_after_index"]["date_range"]["median"],
         pg["queries_after_index"]["date_range"]["median"]),
    ]  # fmt: skip
    fig, axes = plt.subplots(1, len(metrics), figsize=(14, 4.2))
    for ax, (title, a, b) in zip(axes, metrics):
        bars = ax.bar(["SQLite", "PostgreSQL"], [a, b], color=[SERIES[0], SERIES[1]],
                      width=0.55, edgecolor=SURFACE, linewidth=2)  # fmt: skip
        _style(ax, title, "с")
        _label_bars(ax, bars, "{:.3f}" if max(a, b) < 1 else "{:.1f}")
    fig.suptitle("SQLite и PostgreSQL", x=0.01, ha="left", color=INK)
    fig.tight_layout()
    fig.savefig(config.IMG_DIR / "dbms.png", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "worker":
        print(json.dumps(worker(sys.argv[2])))
    else:
        {"formats": bench_formats, "schema": bench_schema, "libs": bench_libs, "plots": plots}[cmd]()
