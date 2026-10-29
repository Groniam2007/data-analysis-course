-- Схема файловой базы dataset.db (SQLite), третья нормальная форма.
-- Таблицы STRICT: SQLite проверяет типы, а не только рекомендует их.
-- Логические значения хранятся как 0/1, даты - как текст ГГГГ-ММ-ДД.
-- Справочные значения вынесены в отдельные таблицы, списки из одной ячейки
-- (alternatenames, cc2) - в таблицы «один ко многим».

CREATE TABLE regions (
    region_id       INTEGER     PRIMARY KEY,
    region_name     TEXT  NOT NULL UNIQUE
) STRICT;

CREATE TABLE income_levels (
    income_level_id   INTEGER    PRIMARY KEY,
    income_level_name TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE countries (
    country_code    TEXT         PRIMARY KEY,
    iso3            TEXT         NOT NULL UNIQUE,
    country_name    TEXT NOT NULL,
    region_id       INTEGER     REFERENCES regions(region_id),
    income_level_id INTEGER     REFERENCES income_levels(income_level_id)
) STRICT;

CREATE TABLE feature_classes (
    feature_class   TEXT         PRIMARY KEY,
    description     TEXT NOT NULL
) STRICT;

-- Класс объекта определяется его кодом (PPL всегда относится к классу P),
-- поэтому класс хранится здесь, а не в geonames. Строка с кодом NULL
-- описывает объекты, у которых известен только класс.
CREATE TABLE feature_codes (
    feature_code_id INTEGER     PRIMARY KEY,
    feature_code    TEXT,
    feature_class   TEXT         NOT NULL REFERENCES feature_classes(feature_class),
    UNIQUE (feature_class, feature_code)
) STRICT;

CREATE TABLE timezones (
    timezone_id     INTEGER     PRIMARY KEY,
    timezone_name   TEXT  NOT NULL UNIQUE
) STRICT;

CREATE TABLE geonames (
    geonameid              INTEGER          PRIMARY KEY,
    name                   TEXT     NOT NULL,
    asciiname              TEXT,
    latitude               REAL NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude              REAL NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    feature_code_id        INTEGER         REFERENCES feature_codes(feature_code_id),
    country_code           TEXT             REFERENCES countries(country_code),
    admin1_code            TEXT,
    admin2_code            TEXT,
    admin3_code            TEXT,
    admin4_code            TEXT,
    population             INTEGER           CHECK (population > 0),
    elevation              INTEGER,
    dem                    INTEGER,
    timezone_id            INTEGER         REFERENCES timezones(timezone_id),
    modification_date      TEXT             NOT NULL,
    population_outlier     INTEGER          NOT NULL CHECK (population_outlier IN (0, 1)),
    elevation_outlier      INTEGER          NOT NULL CHECK (elevation_outlier IN (0, 1)),
    elevation_unit_suspect INTEGER          NOT NULL CHECK (elevation_unit_suspect IN (0, 1))
) STRICT;

CREATE TABLE alternate_names (
    geonameid       INTEGER      NOT NULL REFERENCES geonames(geonameid),
    alt_name        TEXT         NOT NULL,
    PRIMARY KEY (geonameid, alt_name)
) STRICT;

CREATE TABLE geoname_cc2 (
    geonameid       INTEGER      NOT NULL REFERENCES geonames(geonameid),
    country_code    TEXT         NOT NULL REFERENCES countries(country_code),
    PRIMARY KEY (geonameid, country_code)
) STRICT;

-- Показатели здоровья: ключ - пара «страна, год». Наименование, регион и
-- уровень дохода зависят только от страны и вынесены в countries.
CREATE TABLE health_indicators (
    country_code                  TEXT     NOT NULL REFERENCES countries(country_code),
    year                          INTEGER NOT NULL CHECK (year BETWEEN 1960 AND 2100),
    population                    INTEGER,
    population_growth             REAL,
    urban_pct                     REAL,
    life_expectancy               REAL,
    life_expectancy_male          REAL,
    life_expectancy_female        REAL,
    death_rate                    REAL,
    birth_rate                    REAL,
    health_expenditure_pct_gdp    REAL,
    health_expenditure_per_capita REAL,
    physicians_per_1000           REAL,
    hospital_beds_per_1000        REAL,
    nurses_per_1000               REAL,
    under5_mortality              REAL,
    neonatal_mortality            REAL,
    maternal_mortality            REAL,
    tb_incidence                  REAL,
    hiv_incidence                 REAL,
    communicable_death_pct        REAL,
    noncommunicable_death_pct     REAL,
    smoking_male                  REAL,
    smoking_female                REAL,
    alcohol_per_capita            REAL,
    diabetes_pct                  REAL,
    safe_water_pct                REAL,
    sanitation_pct                REAL,
    handwashing_pct               REAL,
    immunization_measles          REAL,
    immunization_dpt              REAL,
    immunization_bcg              REAL,
    immunization_pol3             REAL,
    immunization_hib3             REAL,
    stunting_pct                  REAL,
    wasting_pct                   REAL,
    overweight_children_pct       REAL,
    undernourishment_pct          REAL,
    fertility_rate                REAL,
    adolescent_fertility          REAL,
    prenatal_care_pct             REAL,
    gdp_per_capita                REAL,
    gdp_per_capita_ppp            REAL,
    poverty_rate                  REAL,
    PRIMARY KEY (country_code, year)
) STRICT;

CREATE TABLE indicators (
    indicator_code  TEXT  PRIMARY KEY,
    indicator_name  TEXT         NOT NULL
) STRICT;

CREATE TABLE wdi_values (
    country_code    TEXT             NOT NULL REFERENCES countries(country_code),
    indicator_code  TEXT      NOT NULL REFERENCES indicators(indicator_code),
    year            INTEGER         NOT NULL,
    value           REAL NOT NULL,
    PRIMARY KEY (country_code, indicator_code, year)
) STRICT;
