"""Функции очистки. Каждая принимает и возвращает таблицу polars.

Блокнот notebooks/02_cleaning.ipynb и сценарий src/pipeline.py вызывают одни и
те же функции, поэтому их результаты совпадают.
"""

import logging
import re

import polars as pl
import pycountry

from src import config

log = logging.getLogger(__name__)

# Сводка «было - стало» по каждому этапу; заполняется по ходу работы.
STATS: list[dict] = []

STRING_COLUMNS = [
    "name", "asciiname", "alternatenames", "feature_class", "feature_code",
    "country_code", "cc2", "admin1_code", "admin2_code", "admin3_code",
    "admin4_code", "timezone",
]  # fmt: skip
CATEGORY_COLUMNS = [
    "feature_class", "feature_code", "feature_code_group", "country_code",
    "admin1_code", "admin2_code", "admin3_code", "admin4_code", "timezone",
]  # fmt: skip
SERVICE_VALUES = ["-", "--", "?", "n/a", "na", "null", "none", "нет данных"]


def _record(step: str, before: int, after: int, changed: int = 0, **extra) -> None:
    removed = before - after
    STATS.append(
        {"step": step, "rows_in": before, "rows_out": after,
         "changed": changed, "removed": removed, **extra}  # fmt: skip
    )
    log.info(
        "%s: обработано %d, изменено %d, удалено %d, осталось %d",
        step, before, changed, removed, after,
    )  # fmt: skip


def reset_stats() -> None:
    STATS.clear()


def stats_frame() -> pl.DataFrame:
    return pl.DataFrame(STATS)


def _changed(old: pl.Series, new: pl.Series) -> int:
    """Число позиций, где значение изменилось (с учётом пропусков)."""
    return int((old.ne_missing(new)).sum())


# ---------------------------------------------------------------- имена и типы


def normalize_column_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)
    return re.sub(r"[^0-9a-z_]", "", name)


def rename_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Нижний регистр, пробелы в подчёркивания, без служебных символов."""
    mapping = {c: normalize_column_name(c) for c in df.columns}
    out = df.rename(mapping)
    _record("rename_columns", df.height, out.height,
            changed=sum(k != v for k, v in mapping.items()))  # fmt: skip
    return out


def cast_types(df: pl.DataFrame) -> pl.DataFrame:
    """Явное приведение типов; неразобранные значения становятся пропусками.

    Работает и со строковым входом (scan_csv без схемы), и с типизированным
    Parquet: категориальные столбцы на время очистки переводятся в строки.
    """
    date = pl.col("modification_date")
    dtype = df.schema["modification_date"]
    if dtype == pl.String:
        date = date.str.to_date("%Y-%m-%d", strict=False)
    elif isinstance(dtype, pl.Datetime):
        date = date.dt.date()
    numeric = {
        "geonameid": pl.Int32, "latitude": pl.Float32, "longitude": pl.Float32,
        "population": pl.Int64, "elevation": pl.Int16, "dem": pl.Int16,
    }  # fmt: skip
    out = df.with_columns(
        *[pl.col(c).cast(pl.String).cast(t, strict=False) for c, t in numeric.items()],
        *[pl.col(c).cast(pl.String) for c in STRING_COLUMNS],
        date.cast(pl.Date).alias("modification_date"),
    )
    failed = {
        c: int((df[c].is_not_null() & out[c].is_null()).sum())
        for c in [*numeric, "modification_date"]
    }
    out = out.filter(pl.col("geonameid").is_not_null())
    _record("cast_types", df.height, out.height, changed=sum(failed.values()),
            unparsed=str({k: v for k, v in failed.items() if v}))  # fmt: skip
    return out


# ------------------------------------------------------------------- пропуски


def replace_hidden_missing(df: pl.DataFrame) -> pl.DataFrame:
    """Значения-заменители приводятся к null.

    population = 0 - в GeoNames означает «не указано», а не «никто не живёт»;
    dem = -9999 - код отсутствия данных цифровой модели рельефа SRTM;
    пустые строки, строки из пробелов и служебные отметки вида «-», «n/a».
    """
    service = pl.lit(SERVICE_VALUES)
    out = df.with_columns(
        *[
            pl.when(
                pl.col(c).str.strip_chars().str.len_chars().eq(0)
                | pl.col(c).str.strip_chars().str.to_lowercase().is_in(service)
            )
            .then(None)
            .otherwise(pl.col(c))
            .alias(c)
            for c in STRING_COLUMNS
        ],
        pl.when(pl.col("population") == 0).then(None)
        .otherwise(pl.col("population")).alias("population"),
        pl.when(pl.col("dem") == config.DEM_NODATA).then(None)
        .otherwise(pl.col("dem")).alias("dem"),
    )  # fmt: skip
    per_col = {c: _changed(df[c], out[c]) for c in df.columns if c in out.columns}
    per_col = {k: v for k, v in per_col.items() if v}
    _record("replace_hidden_missing", df.height, out.height,
            changed=sum(per_col.values()), detail=str(per_col))  # fmt: skip
    return out


def missing_profile(df: pl.DataFrame) -> pl.DataFrame:
    """Доля пропусков по каждому столбцу."""
    n = df.height
    return (
        df.null_count()
        .transpose(include_header=True, header_name="column", column_names=["nulls"])
        .with_columns((pl.col("nulls") / n).round(4).alias("share"))
        .sort("share", descending=True)
    )


def missing_by_group(df: pl.DataFrame, column: str, by: str) -> pl.DataFrame:
    """Сосредоточены ли пропуски столбца в определённой группе записей."""
    return (
        df.group_by(by)
        .agg(
            pl.len().alias("rows"),
            pl.col(column).is_null().mean().round(4).alias(f"{column}_missing_share"),
        )
        .sort("rows", descending=True)
    )


# ------------------------------------------------------------------- дубликаты


def drop_full_duplicates(df: pl.DataFrame) -> pl.DataFrame:
    out = df.unique(maintain_order=True)
    _record("drop_full_duplicates", df.height, out.height)
    return out


def drop_key_duplicates(
    df: pl.DataFrame, key: list[str] = config.DUPLICATE_KEY
) -> pl.DataFrame:
    """Одна сущность под разными geonameid: остаётся последняя по дате запись."""
    out = (
        df.sort(["modification_date", "geonameid"], descending=True, nulls_last=True)
        .unique(subset=key, keep="first", maintain_order=True)
        .sort("geonameid")
    )
    _record("drop_key_duplicates", df.height, out.height, key=",".join(key))
    return out


# ------------------------------------------------------------------ категории


def _split_codes(col: str) -> pl.Expr:
    return (
        pl.col(col).str.split(";")
        .list.eval(pl.element().str.strip_chars().str.to_uppercase())
        .list.eval(pl.element().filter(pl.element().str.len_chars() > 0))
        .list.unique().list.sort()
    )  # fmt: skip


def normalize_categories(df: pl.DataFrame) -> pl.DataFrame:
    """Краевые пробелы, единый регистр кодов, упорядоченный список cc2."""
    codes = ["feature_class", "feature_code", "country_code"]
    admin = ["admin1_code", "admin2_code", "admin3_code", "admin4_code"]
    cc2 = _split_codes("cc2")
    out = df.with_columns(
        *[pl.col(c).str.strip_chars().str.to_uppercase() for c in codes],
        *[pl.col(c).str.strip_chars() for c in admin],
        pl.col("timezone").str.strip_chars(),
        pl.when(cc2.list.len() > 0).then(cc2.list.join(";")).alias("cc2"),
    )
    changed = sum(_changed(df[c], out[c]) for c in [*codes, *admin, "timezone", "cc2"])
    _record("normalize_categories", df.height, out.height, changed=changed)
    return out


def merge_rare_categories(
    df: pl.DataFrame, column: str = "feature_code",
    min_count: int = config.RARE_CATEGORY_MIN,
) -> pl.DataFrame:  # fmt: skip
    """Редкие коды объединяются в группу «<класс>.OTHER» в отдельном столбце.

    Исходный код сохраняется: он нужен для связи со справочником в базе.
    """
    counts = df.group_by(column).len()
    rare = counts.filter(pl.col("len") < min_count)[column]
    out = df.with_columns(
        pl.when(pl.col(column).is_in(rare.implode()))
        .then(pl.col("feature_class") + ".OTHER")
        .otherwise(pl.col(column))
        .alias(f"{column}_group")
    )
    changed = int(out[column].is_in(rare.implode()).sum())
    _record("merge_rare_categories", df.height, out.height, changed=changed,
            rare_categories=rare.len())  # fmt: skip
    return out


# ------------------------------------------------------------------- текст


def _clean_text_expr(e: pl.Expr) -> pl.Expr:
    return (
        e.str.replace_all(r"<[^>]+>", " ")
        .str.replace_all(r"[\s ]+", " ")
        .str.strip_chars()
    )


def clean_text(df: pl.DataFrame) -> pl.DataFrame:
    """Разметка, повторные пробелы, пустые и повторяющиеся альтернативные названия.

    alternatenames - список через «;»: каждое название чистится отдельно,
    пустые и повторы внутри списка отбрасываются, порядок сохраняется.
    """
    alt = (
        pl.col("alternatenames").str.split(";")
        .list.eval(_clean_text_expr(pl.element()))
        .list.eval(pl.element().filter(pl.element().str.len_chars() > 0))
        .list.unique(maintain_order=True)
    )  # fmt: skip
    out = df.with_columns(
        _clean_text_expr(pl.col("name")).alias("name"),
        _clean_text_expr(pl.col("asciiname")).alias("asciiname"),
        pl.when(alt.list.len() > 0).then(alt.list.join(";")).alias("alternatenames"),
    ).with_columns(
        pl.when(pl.col(c).str.len_chars() > 0).then(pl.col(c)).alias(c)
        for c in ["name", "asciiname"]
    )
    changed = sum(_changed(df[c], out[c]) for c in ["name", "asciiname", "alternatenames"])
    out = out.filter(pl.col("name").is_not_null())
    _record("clean_text", df.height, out.height, changed=changed)
    return out


# ----------------------------------------------------- единицы и ограничения


def check_units(df: pl.DataFrame) -> pl.DataFrame:
    """Все высоты в источнике заданы в метрах, но часть значений elevation
    похожа на футы: отношение к высоте по модели рельефа dem около 3,28.
    Пересчёт без подтверждения недопустим, поэтому такие записи помечаются.
    """
    ratio = pl.col("elevation").cast(pl.Float64) / pl.col("dem").cast(pl.Float64)
    out = df.with_columns(
        (pl.col("dem").fill_null(0) > 50)
        .and_(ratio.is_between(3.0, 3.6))
        .fill_null(False)
        .alias("elevation_unit_suspect")
    )
    _record("check_units", df.height, out.height,
            changed=int(out["elevation_unit_suspect"].sum()))  # fmt: skip
    return out


def check_domain(df: pl.DataFrame) -> pl.DataFrame:
    """Предметные ограничения: координаты, неотрицательность, даты, высоты."""
    lat, lon = config.LAT_RANGE, config.LON_RANGE
    bad_coord = ~pl.col("latitude").is_between(*lat) | ~pl.col("longitude").is_between(*lon)
    out = df.filter(~bad_coord.fill_null(True))
    removed_coord = df.height - out.height
    before = out
    out = out.with_columns(
        pl.when(pl.col("population").is_between(0, config.POPULATION_MAX))
        .then(pl.col("population")).alias("population"),
        pl.when(pl.col("elevation").is_between(*config.ELEVATION_RANGE))
        .then(pl.col("elevation")).alias("elevation"),
        pl.when(pl.col("dem").is_between(*config.ELEVATION_RANGE))
        .then(pl.col("dem")).alias("dem"),
    )  # fmt: skip
    changed = sum(_changed(before[c], out[c]) for c in ["population", "elevation", "dem"])
    bad_date = ~pl.col("modification_date").is_between(config.DATE_MIN, config.DATE_MAX)
    n_bad_date = int(out.select(bad_date.fill_null(True).sum()).item())
    out = out.filter(~bad_date.fill_null(True))
    _record("check_domain", df.height, out.height, changed=changed,
            bad_coordinates=removed_coord, bad_dates=n_bad_date)  # fmt: skip
    return out


# ------------------------------------------------------------------- выбросы


def _iqr_bounds(s: pl.Series, k: float = 1.5) -> tuple[float, float]:
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    return q1 - k * (q3 - q1), q3 + k * (q3 - q1)


def _sigma_bounds(s: pl.Series, k: float = 3.0) -> tuple[float, float]:
    return s.mean() - k * s.std(), s.mean() + k * s.std()


def outlier_report(df: pl.DataFrame) -> pl.DataFrame:
    """Сопоставление правил IQR, трёх сигм и предметного правила.

    Население сильно скошено, поэтому правила применяются к его логарифму.
    """
    pop = df.select(pl.col("population").drop_nulls().log10()).to_series()
    diff = df.select(
        (pl.col("elevation") - pl.col("dem")).drop_nulls().cast(pl.Float64)
    ).to_series()
    rows = []
    for name, s, domain in [
        ("log10(population)", pop, int(df.select(_population_rule().sum()).item())),
        ("elevation - dem, м", diff, int(df.select(_elevation_rule().sum()).item())),
    ]:
        for method, (lo, hi) in [("IQR 1.5", _iqr_bounds(s)), ("3 sigma", _sigma_bounds(s))]:
            rows.append({"variable": name, "method": method, "low": round(lo, 2),
                         "high": round(hi, 2),
                         "flagged": int(((s < lo) | (s > hi)).sum())})  # fmt: skip
        rows.append({"variable": name, "method": "предметное правило",
                     "low": None, "high": None, "flagged": domain})  # fmt: skip
    return pl.DataFrame(rows)


def _population_rule() -> pl.Expr:
    # У гор, рек, зданий и подводных объектов населения быть не может: значение
    # больше 10 млн у них - ошибка (например, «гора» в Индии с 1,7 млрд жителей).
    return (
        pl.col("feature_class").is_in(["H", "R", "S", "T", "U", "V"])
        & (pl.col("population") > 10_000_000)
    ).fill_null(False)


def _elevation_rule() -> pl.Expr:
    # Высота объекта и высота рельефа в той же точке расходятся больше чем на
    # километр - почти наверняка ошибка ввода или неверные координаты.
    return ((pl.col("elevation") - pl.col("dem")).abs() > 1000).fill_null(False)


def flag_outliers(df: pl.DataFrame) -> pl.DataFrame:
    """Выбросы не удаляются, а помечаются: на скошенном распределении населения
    формальные правила объявляют выбросами все крупные города."""
    out = df.with_columns(
        _population_rule().alias("population_outlier"),
        _elevation_rule().alias("elevation_outlier"),
    )
    changed = int(out["population_outlier"].sum() + out["elevation_outlier"].sum())
    _record("flag_outliers", df.height, out.height, changed=changed)
    return out


# ------------------------------------------------------ справочник стран


def build_countries(geonames: pl.DataFrame, health: pl.DataFrame) -> pl.DataFrame:
    """Справочник стран: все коды ISO2 из geonames (включая cc2) + ISO3 и
    атрибуты из health_indicators. Связь источников идёт через ISO3."""
    legacy3 = {v[0]: k for k, v in config.LEGACY_COUNTRIES.items()}
    health_iso2 = []
    for iso3 in health["country_code"].unique().to_list():
        c = pycountry.countries.get(alpha_3=iso3)
        health_iso2.append(c.alpha_2 if c is not None else legacy3.get(iso3))
    codes = (
        pl.concat([
            geonames.select(pl.col("country_code").alias("c")),
            geonames.select(pl.col("cc2").str.split(";").alias("c")).explode("c"),
            pl.DataFrame({"c": health_iso2}, schema={"c": pl.String}),
        ])
        .drop_nulls().unique().sort("c")["c"].to_list()
    )  # fmt: skip
    rows = []
    for iso2 in codes:
        c = pycountry.countries.get(alpha_2=iso2)
        if c is not None:
            rows.append((iso2, c.alpha_3, c.name))
        elif iso2 in config.LEGACY_COUNTRIES:
            rows.append((iso2, *config.LEGACY_COUNTRIES[iso2]))
        else:
            rows.append((iso2, None, None))
    dim = pl.DataFrame(
        rows, schema={"iso2": pl.String, "iso3": pl.String, "country_name": pl.String},
        orient="row",
    )  # fmt: skip
    attrs = health.group_by("country_code").agg(
        pl.col("country_name").last().alias("wb_name"),
        pl.col("region").last(), pl.col("income_level").last(),
    )  # fmt: skip
    out = dim.join(attrs, left_on="iso3", right_on="country_code", how="left")
    unmatched = attrs.filter(~pl.col("country_code").is_in(dim["iso3"].implode()))
    _record("build_countries", dim.height, out.height,
            health_countries=attrs.height,
            health_unmatched=unmatched.height,
            without_health=int(out["region"].is_null().sum()))  # fmt: skip
    return out.with_columns(
        pl.coalesce("wb_name", "country_name").alias("country_name")
    ).drop("wb_name")


def join_countries(df: pl.DataFrame, countries: pl.DataFrame) -> pl.DataFrame:
    """Левое соединение с проверкой числа строк до и после."""
    dim = countries.select(
        pl.col("iso2").alias("country_code"), "country_name", "region", "income_level"
    )
    assert dim["country_code"].is_unique().all(), "ключ справочника не уникален"
    out = df.join(dim, on="country_code", how="left")
    if out.height != df.height:
        raise ValueError(f"соединение размножило строки: {df.height} -> {out.height}")
    unmatched = int((out["country_code"].is_not_null() & out["country_name"].is_null()).sum())
    _record("join_countries", df.height, out.height,
            unmatched=unmatched,
            no_country_code=int(out["country_code"].is_null().sum()))  # fmt: skip
    return out


# ------------------------------------------------- 1НФ: списки в отдельные таблицы


def explode_alternate_names(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.select("geonameid", pl.col("alternatenames").str.split(";").alias("alt_name"))
        .explode("alt_name")
        .drop_nulls()
        .unique(maintain_order=True)
    )


def explode_cc2(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.select("geonameid", pl.col("cc2").str.split(";").alias("country_code"))
        .explode("country_code")
        .drop_nulls()
        .unique(maintain_order=True)
    )


def finalize(df: pl.DataFrame) -> pl.DataFrame:
    """Итоговые типы: повторяющиеся значения - в категории."""
    return df.with_columns(pl.col(c).cast(pl.Categorical) for c in CATEGORY_COLUMNS)


GEONAMES_STEPS = [
    rename_columns,
    cast_types,
    replace_hidden_missing,
    drop_full_duplicates,
    drop_key_duplicates,
    normalize_categories,
    merge_rare_categories,
    clean_text,
    check_units,
    check_domain,
    flag_outliers,
]


# ------------------------------------------------- вспомогательные источники


def clean_health(df: pl.DataFrame) -> pl.DataFrame:
    """Показатели здоровья: имена, категории, пустые столбцы, доли 0..100."""
    before = df.height
    df = df.rename({c: normalize_column_name(c) for c in df.columns})
    text = ["country_code", "country_name", "region", "income_level"]
    raw = df
    df = df.with_columns(
        pl.col(c).str.strip_chars().str.replace_all(r"\s+", " ") for c in text
    )
    trimmed = sum(_changed(raw[c], df[c]) for c in text)
    empty = [c for c in df.columns if df[c].null_count() == df.height]
    df = df.drop(empty).with_columns(pl.col("year").cast(pl.Int16))
    pct = [c for c in df.columns if c.endswith("_pct")]
    out_of_range = sum(int(((df[c] < 0) | (df[c] > 100)).sum()) for c in pct)
    df = df.with_columns(
        pl.when(pl.col(c).is_between(0, 100)).then(pl.col(c)).alias(c) for c in pct
    )
    df = df.unique(subset=["country_code", "year"], keep="last", maintain_order=True)
    _record("clean_health", before, df.height, changed=trimmed + out_of_range,
            trimmed_categories=trimmed, dropped_empty_columns=",".join(empty))  # fmt: skip
    return df


def clean_wdi(df: pl.DataFrame, countries: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """World Development Indicators: широкий формат в длинный.

    Возвращает справочник индикаторов и значения только по странам из
    справочника; региональные агрегаты (Arab World и т. п.) не сопоставляются.
    """
    before = df.height
    df = df.drop([c for c in df.columns if not c.strip()])
    df = df.rename({c: normalize_column_name(c) for c in df.columns})
    years = [c for c in df.columns if c.isdigit()]
    indicators = (
        df.select("indicator_code", "indicator_name")
        .unique(subset="indicator_code", keep="first", maintain_order=True)
        .with_columns(_clean_text_expr(pl.col("indicator_name")))
    )
    long = (
        df.select("country_code", "indicator_code", *years)
        .unpivot(index=["country_code", "indicator_code"], variable_name="year")
        .with_columns(pl.col("year").cast(pl.Int16), pl.col("value").cast(pl.Float64, strict=False))
        .drop_nulls("value")
        .unique(subset=["country_code", "indicator_code", "year"], keep="last")
    )
    iso3 = countries["iso3"].drop_nulls().implode()
    matched = long.filter(pl.col("country_code").is_in(iso3))
    areas = long.filter(~pl.col("country_code").is_in(iso3))["country_code"].n_unique()
    _record("clean_wdi", long.height, matched.height, wide_rows=before,
            unmatched_areas=areas)  # fmt: skip
    return indicators, matched.sort(["country_code", "indicator_code", "year"])
