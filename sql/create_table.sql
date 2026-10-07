CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.restaurant_inspections (
    camis                    VARCHAR,
    dba                      VARCHAR,
    boro                     VARCHAR,
    building                 VARCHAR,
    street                   VARCHAR,
    zipcode                  VARCHAR,
    phone                    VARCHAR,
    cuisine_description      VARCHAR,
    inspection_date          TIMESTAMP,
    action                   VARCHAR,
    violation_code           VARCHAR,
    violation_description    TEXT,
    critical_flag            VARCHAR,
    score                    INTEGER,
    grade                    VARCHAR,
    grade_date               TIMESTAMP,
    record_date              TIMESTAMP,
    inspection_type          VARCHAR,
    community_board          INTEGER,
    council_district         INTEGER,
    census_tract             INTEGER,
    bin                      BIGINT,
    bbl                      BIGINT,
    nta                      VARCHAR
);