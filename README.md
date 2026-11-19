# Введение в анализ данных и программирование на Python

Учебный проект курса. Лабораторная работа № 1 — базовая работа с Python,
обработка и очистка данных: от исходного CSV до очищенного набора в Parquet,
SQLite и PostgreSQL.

Набор данных — GeoNames (11,5 млн географических объектов) и показатели стран
Всемирного банка; описание столбцов и дефектов — в [DATASET.md](DATASET.md).

## Требования

- Python **3.12.14** (`requires-python = "==3.12.14"`). Для Windows у ветки 3.12
  python.org больше не выпускает установщики, поэтому интерпретатор ставится
  готовой сборкой: `pip install uv` и `uv python install 3.12.14`.
- poetry 2.x, git.
- PostgreSQL 16+ для шага загрузки в серверную СУБД.

## Развёртывание окружения

```powershell
git clone https://github.com/Groniam2007/data-analysis-course.git
cd data-analysis-course
poetry config virtualenvs.in-project true --local   # уже задано в poetry.toml
poetry env use <путь к python 3.12.14>
poetry install                                      # точный состав из poetry.lock
.venv\Scripts\Activate.ps1
python -c "import sys; print(sys.executable, sys.prefix != sys.base_prefix)"
```

Без poetry: `py -3.12 -m venv .venv`, активация, `pip install -r requirements.txt`.

Реквизиты СУБД: скопировать `.env.example` в `.env` и заполнить.

## Данные

Исходные файлы кладутся в `data/raw/` и не изменяются ни одной программой:

```
data/raw/geonames.csv            # GeoNames allCountries в CSV
data/raw/health_indicators.csv   # показатели здоровья по странам и годам
data/raw/data.csv                # World Development Indicators
```

Движение данных: `raw` → `interim` → `processed`.

| Этап | Команда | Результат |
|---|---|---|
| отладочный срез | `--stage sample` | `data/interim/geonames_sample.csv` (200 тыс. строк) |
| сериализация | `--stage serialize` | `data/interim/geonames.parquet` |
| схема типов | `--stage typed` | `data/interim/geonames_typed.parquet` |
| очистка | `--stage clean` | `data/processed/clean.parquet` и справочники |

## Запуск

```powershell
python -m src.pipeline                 # полный набор, все этапы (~2,5 мин)
python -m src.pipeline --sample        # то же на срезе
python -m src.pipeline --stage clean --sample
python -m src.db --target sqlite       # загрузка в data/processed/dataset.db
python -m src.db --target postgres     # загрузка в PostgreSQL из .env
python -m src.benchmarks formats|schema|libs|plots
```

Журналы пишутся в `logs/`, результаты замеров — в `docs/results/`, диаграммы —
в `docs/img/`.

## Структура

```
src/config.py      пути, параметры чтения, схема типов, предметные границы
src/io_utils.py    чтение форматов, потоковый срез, замеры времени
src/cleaning.py    функции очистки (общие для блокнота и сценария)
src/pipeline.py    сквозной сценарий с интерфейсом командной строки
src/db.py          схема в 3НФ, загрузка в SQLite и PostgreSQL, запросы
src/benchmarks.py  замеры шагов 9–11 и диаграммы
sql/               DDL обеих СУБД
notebooks/         01_overview, 02_cleaning, 03_colab
docs/              ER-диаграмма, диаграммы и результаты замеров
```
