"""Пути, параметры чтения и схема типов набора данных."""

import csv
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
LOGS_DIR = ROOT / "logs"
DOCS_DIR = ROOT / "docs"
RESULTS_DIR = DOCS_DIR / "results"
IMG_DIR = DOCS_DIR / "img"
SQL_DIR = ROOT / "sql"

# исходные файлы (каталог raw только для чтения)
GEONAMES_CSV = RAW_DIR / "geonames.csv"
HEALTH_CSV = RAW_DIR / "health_indicators.csv"
WDI_CSV = RAW_DIR / "data.csv"

# промежуточные файлы
SAMPLE_CSV = INTERIM_DIR / "geonames_sample.csv"
SOURCE_PARQUET = INTERIM_DIR / "geonames.parquet"
SOURCE_FEATHER = INTERIM_DIR / "geonames.feather"
SOURCE_PICKLE = INTERIM_DIR / "geonames.pkl"
TYPED_PARQUET = INTERIM_DIR / "geonames_typed.parquet"

# результат очистки
CLEAN_PARQUET = PROCESSED_DIR / "clean.parquet"
ALT_NAMES_PARQUET = PROCESSED_DIR / "alternate_names.parquet"
CC2_PARQUET = PROCESSED_DIR / "geoname_cc2.parquet"
COUNTRIES_PARQUET = PROCESSED_DIR / "countries.parquet"
HEALTH_PARQUET = PROCESSED_DIR / "health_indicators.parquet"
WDI_PARQUET = PROCESSED_DIR / "wdi_values.parquet"
INDICATORS_PARQUET = PROCESSED_DIR / "indicators.parquet"

def variant(path: Path, sample: bool) -> Path:
    """Путь результата для отладочного среза: geonames.parquet -> geonames_sample.parquet."""
    return path.with_stem(f"{path.stem}_sample") if sample else path


SAMPLE_ROWS = 200_000
SEED = 42

# Файл geonames.csv записан без экранирования: в каждой строке ровно 18 запятых,
# а символ " встречается внутри названий (Dar"yoi Amu). Поэтому кавычки отключены.
# Код страны NA (Намибия) нельзя считать пропуском - отключаем значения по умолчанию.
GEONAMES_READ_OPTIONS = {
    "sep": ",",
    "quoting": csv.QUOTE_NONE,
    "encoding": "utf-8",
    "keep_default_na": False,
    "na_values": [""],
}

# Схема типов. Границы выбраны по предметной области, а не по текущей выборке:
# geonameid растёт с каждым новым объектом, но до 2,1 млрд ему далеко - int32;
# население континентов и Земли (до ~8 млрд) не помещается в int32 - int64;
# высоты от -11 000 м (Марианская впадина) до 8 849 м (Эверест) - int16;
# координаты заданы с 5 знаками (около 1 м), float32 даёт 7 значащих цифр.
GEONAMES_SCHEMA = {
    "geonameid": "int32",
    "name": "string[pyarrow]",
    "asciiname": "string[pyarrow]",
    "alternatenames": "string[pyarrow]",
    "latitude": "float32",
    "longitude": "float32",
    "feature class": "category",
    "feature code": "category",
    "country code": "category",
    "cc2": "string[pyarrow]",
    "admin1 code": "category",
    "admin2 code": "category",
    "admin3 code": "category",
    "admin4 code": "category",
    "population": "int64",
    "elevation": "Int16",
    "dem": "int16",
    "timezone": "category",
}
GEONAMES_DATE_COLUMNS = ["modification date"]

# Предметные ограничения
LAT_RANGE = (-90.0, 90.0)
LON_RANGE = (-180.0, 180.0)
ELEVATION_RANGE = (-11_000, 8_850)
POPULATION_MAX = 8_200_000_000
DATE_MIN = date(1990, 1, 1)
DATE_MAX = date.today()

# Скрытые пропуски
DEM_NODATA = -9999
RARE_CATEGORY_MIN = 100
DUPLICATE_KEY = ["name", "feature_code", "country_code", "latitude", "longitude"]

# Устаревшие и пользовательские коды стран, которых нет в ISO 3166-1
LEGACY_COUNTRIES = {
    "XK": ("XKX", "Kosovo"),
    "CS": ("SCG", "Serbia and Montenegro"),
    "YU": ("YUG", "Yugoslavia"),
    "AN": ("ANT", "Netherlands Antilles"),
}

FEATURE_CLASSES = {
    "A": "страна, штат, регион",
    "H": "водный объект: река, озеро",
    "L": "парк, территория",
    "P": "населённый пункт",
    "R": "дорога, железная дорога",
    "S": "сооружение, здание, ферма",
    "T": "рельеф: гора, холм, скала",
    "U": "подводный объект",
    "V": "лес, растительность",
}
