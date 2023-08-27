CREATE TABLE IF NOT EXISTS apartments_property (
    url VARCHAR NOT NULL,
    site_last_mod VARCHAR,
    html_contents VARCHAR,
    last_downloaded VARCHAR,
    population BIGINT
    CONSTRAINT url_pk PRIMARY KEY (url));

CREATE TABLE IF NOT EXISTS PARCEL_INFO (
    PARCEL_ID VARCHAR NOT NULL,
    COUNTY VARCHAR,
    CONSTRAINT parcel_pk PRIMARY KEY (PARCEL_ID, COUNTY));


CREATE TABLE IF NOT EXISTS ADDRESS_INFO (
    PARCEL_ID VARCHAR NOT NULL,
    COUNTY VARCHAR,
    STREET_NUM VARCHAR,
    UNIT_NUM VARCHAR,
    STREET_NAME VARCHAR,
    CITY VARCHAR,
    ZIP VARCHAR,
    ZIP_EXTENSION VARCHAR,
    USE_CODE VARCHAR,
    CONSTRAINT parcel_pk PRIMARY KEY (PARCEL_ID, COUNTY));
