-- Схема базы analytics (PostgreSQL), третья нормальная форма.
-- Справочные значения вынесены в отдельные таблицы, списки из одной ячейки
-- (alternatenames, cc2) - в таблицы «один ко многим».

CREATE TABLE regions (
    region_id       SMALLINT     PRIMARY KEY,
    region_name     VARCHAR(80)  NOT NULL UNIQUE
);

CREATE TABLE income_levels (
    income_level_id   SMALLINT    PRIMARY KEY,
    income_level_name VARCHAR(40) NOT NULL UNIQUE
);

CREATE TABLE countries (
    country_code    CHAR(2)      PRIMARY KEY,
    iso3            CHAR(3)      NOT NULL UNIQUE,
    country_name    VARCHAR(100) NOT NULL,
    region_id       SMALLINT     REFERENCES regions(region_id),
    income_level_id SMALLINT     REFERENCES income_levels(income_level_id)
);

CREATE TABLE feature_classes (
    feature_class   CHAR(1)      PRIMARY KEY,
    description     VARCHAR(100) NOT NULL
);

-- Класс объекта определяется его кодом (PPL всегда относится к классу P),
-- поэтому класс хранится здесь, а не в geonames. Строка с кодом NULL
-- описывает объекты, у которых известен только класс.
CREATE TABLE feature_codes (
    feature_code_id SMALLINT     PRIMARY KEY,
    feature_code    VARCHAR(10),
    feature_class   CHAR(1)      NOT NULL REFERENCES feature_classes(feature_class),
    UNIQUE (feature_class, feature_code)
);

CREATE TABLE timezones (
    timezone_id     SMALLINT     PRIMARY KEY,
    timezone_name   VARCHAR(40)  NOT NULL UNIQUE
);

CREATE TABLE geonames (
    geonameid              INTEGER          PRIMARY KEY,
    name                   VARCHAR(200)     NOT NULL,
    asciiname              VARCHAR(200),
    latitude               DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude              DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    feature_code_id        SMALLINT         REFERENCES feature_codes(feature_code_id),
    country_code           CHAR(2)          REFERENCES countries(country_code),
    admin1_code            VARCHAR(20),
    admin2_code            VARCHAR(80),
    admin3_code            VARCHAR(20),
    admin4_code            VARCHAR(20),
    population             BIGINT           CHECK (population > 0),
    elevation              SMALLINT,
    dem                    SMALLINT,
    timezone_id            SMALLINT         REFERENCES timezones(timezone_id),
    modification_date      DATE             NOT NULL,
    population_outlier     BOOLEAN          NOT NULL,
    elevation_outlier      BOOLEAN          NOT NULL,
    elevation_unit_suspect BOOLEAN          NOT NULL
);

CREATE TABLE alternate_names (
    geonameid       INTEGER      NOT NULL REFERENCES geonames(geonameid),
    alt_name        TEXT         NOT NULL,
    PRIMARY KEY (geonameid, alt_name)
);

CREATE TABLE geoname_cc2 (
    geonameid       INTEGER      NOT NULL REFERENCES geonames(geonameid),
    country_code    CHAR(2)      NOT NULL REFERENCES countries(country_code),
    PRIMARY KEY (geonameid, country_code)
);

-- Показатели здоровья: ключ - пара «страна, год». Наименование, регион и
-- уровень дохода зависят только от страны и вынесены в countries.
CREATE TABLE health_indicators (
    country_code                  CHAR(2)  NOT NULL REFERENCES countries(country_code),
    year                          SMALLINT NOT NULL CHECK (year BETWEEN 1960 AND 2100),
    population                    BIGINT,
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
    gdp_per_capita                DOUBLE PRECISION,
    gdp_per_capita_ppp            DOUBLE PRECISION,
    poverty_rate                  REAL,
    PRIMARY KEY (country_code, year)
);

CREATE TABLE indicators (
    indicator_code  VARCHAR(50)  PRIMARY KEY,
    indicator_name  TEXT         NOT NULL
);

CREATE TABLE wdi_values (
    country_code    CHAR(2)          NOT NULL REFERENCES countries(country_code),
    indicator_code  VARCHAR(50)      NOT NULL REFERENCES indicators(indicator_code),
    year            SMALLINT         NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (country_code, indicator_code, year)
);
